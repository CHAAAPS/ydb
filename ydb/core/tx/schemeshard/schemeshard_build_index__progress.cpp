#include "schemeshard_build_index.h"
#include "schemeshard_impl.h"
#include "schemeshard_build_index_helpers.h"
#include "schemeshard_build_index_tx_base.h"

#include <ydb/core/base/table_vector_index.h>
#include <ydb/core/scheme/scheme_types_proto.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/core/tx/datashard/upload_stats.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/tx_proxy/upload_rows.h>

#include <ydb/public/api/protos/ydb_issue_message.pb.h>
#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <ydb/library/yql/public/issue/yql_issue_message.h>
#include <ydb/core/ydb_convert/table_description.h>


namespace NKikimr {
namespace NSchemeShard {

static constexpr const char* Name(TIndexBuildInfo::EState state) noexcept {
    switch (state) {
    case TIndexBuildInfo::EState::Invalid:
        return "Invalid";
    case TIndexBuildInfo::EState::AlterMainTable:
        return "AlterMainTable";
    case TIndexBuildInfo::EState::Locking:
        return "Locking";
    case TIndexBuildInfo::EState::GatheringStatistics:
        return "GatheringStatistics";
    case TIndexBuildInfo::EState::Initiating:
        return "Initiating";
    case TIndexBuildInfo::EState::Filling:
        return "Filling";
    case TIndexBuildInfo::EState::DropTmp:
        return "DropTmp";
    case TIndexBuildInfo::EState::CreateTmp:
        return "CreateTmp";
    case TIndexBuildInfo::EState::Applying:
        return "Applying";
    case TIndexBuildInfo::EState::Unlocking:
        return "Unlocking";
    case TIndexBuildInfo::EState::Done:
        return "Done";
    case TIndexBuildInfo::EState::Cancellation_Applying:
        return "Cancellation_Applying";
    case TIndexBuildInfo::EState::Cancellation_Unlocking:
        return "Cancellation_Unlocking";
    case TIndexBuildInfo::EState::Cancelled:
        return "Cancelled";
    case TIndexBuildInfo::EState::Rejection_Applying:
        return "Rejection_Applying";
    case TIndexBuildInfo::EState::Rejection_Unlocking:
        return "Rejection_Unlocking";
    case TIndexBuildInfo::EState::Rejected:
        return "Rejected";
    }
}

// return count, parts, step
static std::tuple<ui32, ui32, ui32> ComputeKMeansBoundaries(const NSchemeShard::TTableInfo& tableInfo, const TIndexBuildInfo& buildInfo) {
    const auto& kmeans = buildInfo.KMeans;
    Y_ASSERT(kmeans.K != 0);
    Y_ASSERT((kmeans.K & (kmeans.K - 1)) == 0);
    auto pow = [](ui32 k, ui32 l) {
        ui32 r = 1;
        while (l != 0) {
            if (l % 2 != 0) {
                r *= k;
            }
            k *= k;
            l /= 2;
        }
        return r;
    };
    const auto count = pow(kmeans.K, kmeans.Level + 1);
    ui32 step = 1;
    auto parts = count;
    auto shards = tableInfo.GetShard2PartitionIdx().size();
    if (buildInfo.KMeans.Level + 1 == buildInfo.KMeans.Levels || shards <= 1) {
        shards = 1;
        parts = 1;
    }
    for (; shards < parts; parts /= 2) {
        step *= 2;
    }
    for (; parts < shards / 2; parts *= 2) {
        Y_ASSERT(step == 1);
    }
    return {count, parts, step};
}

class TUploadSampleK: public TActorBootstrapped<TUploadSampleK> {
    using TThis = TUploadSampleK;
    using TBase = TActorBootstrapped<TThis>;

protected:
    TString LogPrefix;
    TString TargetTable;

    NDataShard::TUploadRetryLimits Limits;

    TActorId ResponseActorId;  
    ui64 BuildIndexId = 0;
    TIndexBuildInfo::TSampleK::TRows Init;

    std::shared_ptr<NTxProxy::TUploadTypes> Types;
    std::shared_ptr<NTxProxy::TUploadRows> Rows;

    TActorId Uploader;
    ui32 RetryCount = 0;
    ui32 RowsBytes = 0;

    NDataShard::TUploadStatus UploadStatus;

public:
    TUploadSampleK(TString targetTable,
                   const TIndexBuildInfo::TLimits& limits,
                   const TActorId& responseActorId,
                   ui64 buildIndexId,
                   TIndexBuildInfo::TSampleK::TRows&& init)
        : TargetTable(std::move(targetTable))
        , ResponseActorId(responseActorId)
        , BuildIndexId(buildIndexId)
        , Init(std::move(init))
    {
        LogPrefix = TStringBuilder() 
            << "TUploadSampleK: BuildIndexId: " << BuildIndexId 
            << " ResponseActorId: " << ResponseActorId;
        Limits.MaxUploadRowsRetryCount = limits.MaxRetries;
        Y_ASSERT(!Init.empty());
    }

    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::SAMPLE_K_UPLOAD_ACTOR;
    }

    void UploadStatusToMessage(NKikimrIndexBuilder::TEvUploadSampleKResponse& msg) {
        msg.SetUploadStatus(UploadStatus.StatusCode);
        NYql::IssuesToMessage(UploadStatus.Issues, msg.MutableIssues());
    }

    void Describe(IOutputStream& out) const noexcept override {
        out << LogPrefix << Debug();
    }

    TString Debug() const {
        return UploadStatus.ToString();
    }

    void Bootstrap() {
        Rows = std::make_shared<NTxProxy::TUploadRows>();
        Rows->reserve(Init.size());
        std::array<TCell, 2> PrimaryKeys;
        PrimaryKeys[0] = TCell::Make(ui32{0});
        for (ui32 i = 1; auto& [_, row] : Init) {
            RowsBytes += row.size();
            PrimaryKeys[1] = TCell::Make(ui32{i++});
            // TODO(mbkkt) we can avoid serialization of PrimaryKeys every iter
            Rows->emplace_back(TSerializedCellVec{PrimaryKeys}, std::move(row));
        }
        Init = {}; // release memory
        RowsBytes += Rows->size() * TSerializedCellVec::SerializedSize(PrimaryKeys);

        Types = std::make_shared<NTxProxy::TUploadTypes>(3);
        Ydb::Type type;
        type.set_type_id(Ydb::Type::UINT32);
        (*Types)[0] = {NTableIndex::NTableVectorKmeansTreeIndex::LevelTable_ParentIdColumn, type};
        (*Types)[1] = {NTableIndex::NTableVectorKmeansTreeIndex::LevelTable_IdColumn, type};
        type.set_type_id(Ydb::Type::STRING);
        (*Types)[2] = {NTableIndex::NTableVectorKmeansTreeIndex::LevelTable_EmbeddingColumn, type};

        Become(&TThis::StateWork);

        Upload(false);
    }

private:
    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvTxUserProxy::TEvUploadRowsResponse, Handle);
            cFunc(TEvents::TSystem::Wakeup, HandleWakeup);
            default:
                LOG_E("StateWork unexpected event type: " << ev->GetTypeRewrite() << " event: " << ev->ToString());
        }
    }

    void HandleWakeup() {
        LOG_D("Retry upload " << Debug());

        if (Rows) {
            Upload(true);
        }
    }

    void Handle(TEvTxUserProxy::TEvUploadRowsResponse::TPtr& ev) {
        LOG_T("Handle TEvUploadRowsResponse "
              << Debug()
              << " Uploader: " << Uploader.ToString()
              << " ev->Sender: " << ev->Sender.ToString());

        if (Uploader) {
            Y_VERIFY_S(Uploader == ev->Sender,
                       LogPrefix << "Mismatch"
                                 << " Uploader: " << Uploader.ToString()
                                 << " ev->Sender: " << ev->Sender.ToString()
                                 << Debug());
        } else {
            return;
        }

        UploadStatus.StatusCode = ev->Get()->Status;
        UploadStatus.Issues = std::move(ev->Get()->Issues);

        if (UploadStatus.IsRetriable() && RetryCount < Limits.MaxUploadRowsRetryCount) {
            LOG_N("Got retriable error, " << Debug() << " RetryCount: " << RetryCount);

            this->Schedule(Limits.GetTimeoutBackouff(RetryCount), new TEvents::TEvWakeup());
            return;
        }
        TAutoPtr<TEvIndexBuilder::TEvUploadSampleKResponse> response = new TEvIndexBuilder::TEvUploadSampleKResponse;

        response->Record.SetId(BuildIndexId);
        response->Record.SetRowsDelta(Rows->size());
        response->Record.SetBytesDelta(RowsBytes);

        UploadStatusToMessage(response->Record);

        this->Send(ResponseActorId, response.Release());
        this->PassAway();
    }

    void Upload(bool isRetry) {
        if (isRetry) {
            ++RetryCount;
        } else {
            RetryCount = 0;
        }

        auto actor = NTxProxy::CreateUploadRowsInternal(
            SelfId(),
            TargetTable,
            Types,
            Rows,
            NTxProxy::EUploadRowsMode::WriteToTableShadow, // TODO(mbkkt) is it fastest?
            true /*writeToPrivateTable*/);

        Uploader = this->Register(actor);
    }
};

THolder<TEvSchemeShard::TEvModifySchemeTransaction> LockPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.LockTxId), ss->TabletID());
    propose->Record.SetFailOnExist(false);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateLock);
    modifyScheme.SetInternal(true);

    TPath path = TPath::Init(buildInfo.TablePathId, ss);
    modifyScheme.SetWorkingDir(path.Parent().PathString());
    modifyScheme.MutableLockConfig()->SetName(path.LeafName());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> InitiatePropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.InitiateTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetInternal(true);
    modifyScheme.SetWorkingDir(TPath::Init(buildInfo.DomainPathId, ss).PathString());
    modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo.LockTxId));

    if (buildInfo.IsBuildIndex()) {
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateIndexBuild);
        buildInfo.SerializeToProto(ss, modifyScheme.MutableInitiateIndexBuild());
    } else if (buildInfo.IsBuildColumns()) {
        modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCreateColumnBuild);
        buildInfo.SerializeToProto(ss, modifyScheme.MutableInitiateColumnBuild());
    } else {
        Y_ABORT("Unknown operation kind while building InitiatePropose");
    }

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> DropTmpPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    Y_ASSERT(buildInfo.IsBuildVectorIndex());

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.ApplyTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    auto path = TPath::Init(buildInfo.TablePathId, ss).Dive(buildInfo.IndexName);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetInternal(true);
    modifyScheme.SetWorkingDir(path.PathString());

    using namespace NTableIndex::NTableVectorKmeansTreeIndex;
    TString name = PostingTable;
    const char* suffix = buildInfo.KMeans.Level % 2 == 0 ? TmpPostingTableSuffix0 : TmpPostingTableSuffix1;
    name.append(suffix);

    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropTable);
    modifyScheme.MutableDrop()->SetName(name);

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CreateTmpPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    Y_ASSERT(buildInfo.IsBuildVectorIndex());

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.ApplyTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);
    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetInternal(true);

    auto path = TPath::Init(buildInfo.TablePathId, ss);
    const auto& tableInfo = ss->Tables.at(path->PathId);
    NTableIndex::TTableColumns implTableColumns;
    {
        buildInfo.SerializeToProto(ss, modifyScheme.MutableInitiateIndexBuild());
        const auto& indexDesc = modifyScheme.GetInitiateIndexBuild().GetIndex();
        NKikimrScheme::EStatus status;
        TString errStr;
        Y_ABORT_UNLESS(NTableIndex::CommonCheck(tableInfo, indexDesc, path.DomainInfo()->GetSchemeLimits(), false, implTableColumns, status, errStr));
        modifyScheme.ClearInitiateIndexBuild();
    }

    // TODO(mbkkt) for levels greater than zero we need to disable split/merge completely
    // For now it's not guranteed, but very likely
    // But lock is really unconvinient approach (needs to store TxId/etc)
    // So maybe best way to do this is specify something in defintion, that will prevent these operations like IsBackup
    using namespace NTableIndex::NTableVectorKmeansTreeIndex;
    modifyScheme.SetWorkingDir(path.Dive(buildInfo.IndexName).PathString());
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpInitiateBuildIndexImplTable);
    auto& op = *modifyScheme.MutableCreateTable();
    const char* suffix = buildInfo.KMeans.Level % 2 == 0 ? TmpPostingTableSuffix0 : TmpPostingTableSuffix1;
    op = CalcVectorKmeansTreePostingImplTableDesc(tableInfo, tableInfo->PartitionConfig(), implTableColumns, {}, suffix);

    const auto [count, parts, step] = ComputeKMeansBoundaries(*tableInfo, buildInfo);

    auto& config = *op.MutablePartitionConfig();
    config.SetShadowData(true);

    auto& policy = *config.MutablePartitioningPolicy();
    policy.SetSizeToSplit(0); // disable auto split/merge
    policy.SetMinPartitionsCount(parts);
    policy.SetMaxPartitionsCount(parts);
    policy.ClearFastSplitSettings();
    policy.ClearSplitByLoadSettings();

    op.ClearSplitBoundary();
    if (parts <= 1) {
        return propose;
    }
    auto i = buildInfo.KMeans.Parent;
    for (const auto end = i + count;;) {
        i += step;
        if (i >= end) {
            Y_ASSERT(op.SplitBoundarySize() == std::min(count, parts) - 1);
            break;
        }
        auto cell = TCell::Make(i);
        op.AddSplitBoundary()->SetSerializedKeyPrefix(TSerializedCellVec::Serialize({&cell, 1}));
    }
    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> AlterMainTablePropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    Y_ABORT_UNLESS(buildInfo.IsBuildColumns(), "Unknown operation kind while building AlterMainTablePropose");

    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.AlterMainTableTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpAlterTable);
    modifyScheme.SetInternal(true);

    auto path = TPath::Init(buildInfo.TablePathId, ss);
    modifyScheme.SetWorkingDir(path.Parent().PathString());
    modifyScheme.MutableAlterTable()->SetName(path.LeafName());

    for(auto& colInfo : buildInfo.BuildColumns) {
        auto col = modifyScheme.MutableAlterTable()->AddColumns();
        NScheme::TTypeInfo typeInfo;
        TString typeMod;
        Ydb::StatusIds::StatusCode status;
        TString error;
        if (!ExtractColumnTypeInfo(typeInfo, typeMod, colInfo.DefaultFromLiteral.type(), status, error)) {
            // todo gvit fix that
            Y_ABORT("failed to extract column type info");
        }

        col->SetType(NScheme::TypeName(typeInfo, typeMod));
        col->SetName(colInfo.ColumnName);
        col->MutableDefaultFromLiteral()->CopyFrom(colInfo.DefaultFromLiteral);
        col->SetIsBuildInProgress(true);

        if (!colInfo.FamilyName.empty()) {
            col->SetFamilyName(colInfo.FamilyName);
        }

        if (colInfo.NotNull) {
            col->SetNotNull(colInfo.NotNull);
        }

    }

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> ApplyPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.ApplyTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpApplyIndexBuild);
    modifyScheme.SetInternal(true);

    modifyScheme.SetWorkingDir(TPath::Init(buildInfo.DomainPathId, ss).PathString());

    modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo.LockTxId));

    auto& indexBuild = *modifyScheme.MutableApplyIndexBuild();
    indexBuild.SetTablePath(TPath::Init(buildInfo.TablePathId, ss).PathString());

    if (buildInfo.IsBuildIndex()) {
        indexBuild.SetIndexName(buildInfo.IndexName);
    }

    indexBuild.SetSnapshotTxId(ui64(buildInfo.InitiateTxId));
    indexBuild.SetBuildIndexId(ui64(buildInfo.Id));

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> UnlockPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.UnlockTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpDropLock);
    modifyScheme.SetInternal(true);

    modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo.LockTxId));

    TPath path = TPath::Init(buildInfo.TablePathId, ss);
    modifyScheme.SetWorkingDir(path.Parent().PathString());

    auto& lockConfig = *modifyScheme.MutableLockConfig();
    lockConfig.SetName(path.LeafName());

    return propose;
}

THolder<TEvSchemeShard::TEvModifySchemeTransaction> CancelPropose(
    TSchemeShard* ss, const TIndexBuildInfo& buildInfo)
{
    auto propose = MakeHolder<TEvSchemeShard::TEvModifySchemeTransaction>(ui64(buildInfo.ApplyTxId), ss->TabletID());
    propose->Record.SetFailOnExist(true);

    NKikimrSchemeOp::TModifyScheme& modifyScheme = *propose->Record.AddTransaction();
    modifyScheme.SetOperationType(NKikimrSchemeOp::ESchemeOpCancelIndexBuild);
    modifyScheme.SetInternal(true);

    modifyScheme.SetWorkingDir(TPath::Init(buildInfo.DomainPathId, ss).PathString());

    modifyScheme.MutableLockGuard()->SetOwnerTxId(ui64(buildInfo.LockTxId));

    auto& indexBuild = *modifyScheme.MutableCancelIndexBuild();
    indexBuild.SetTablePath(TPath::Init(buildInfo.TablePathId, ss).PathString());
    indexBuild.SetIndexName(buildInfo.IndexName);
    indexBuild.SetSnapshotTxId(ui64(buildInfo.InitiateTxId));
    indexBuild.SetBuildIndexId(ui64(buildInfo.Id));

    return propose;
}

using namespace NTabletFlatExecutor;

struct TSchemeShard::TIndexBuilder::TTxProgress: public TSchemeShard::TIndexBuilder::TTxBase  {
private:
    TIndexBuildId BuildId;

    TDeque<std::tuple<TTabletId, ui64, THolder<IEventBase>>> ToTabletSend;

    template <typename Record>
    TTabletId CommonFillRecord(Record& record, TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        TTabletId shardId = Self->ShardInfos.at(shardIdx).TabletID;
        record.SetTabletId(ui64(shardId));
        record.SetSnapshotTxId(ui64(buildInfo.SnapshotTxId));
        record.SetSnapshotStep(ui64(buildInfo.SnapshotStep));

        auto& shardStatus = buildInfo.Shards.at(shardIdx);
        if (shardStatus.LastKeyAck) {
            TSerializedTableRange range = TSerializedTableRange(shardStatus.LastKeyAck, "", false, false);
            range.Serialize(*record.MutableKeyRange());
        } else {
            shardStatus.Range.Serialize(*record.MutableKeyRange());
        }

        record.SetSeqNoGeneration(Self->Generation());
        record.SetSeqNoRound(++shardStatus.SeqNoRound);
        return shardId;
    }

    void SendSampleKRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        Y_ASSERT(buildInfo.IsBuildVectorIndex());
        auto ev = MakeHolder<TEvDataShard::TEvSampleKRequest>();
        ev->Record.SetId(ui64(BuildId));

        PathIdFromPathId(buildInfo.TablePathId, ev->Record.MutablePathId());        

        ev->Record.SetK(buildInfo.KMeans.K);
        ev->Record.SetMaxProbability(buildInfo.Sample.MaxProbability);

        ev->Record.AddColumns(buildInfo.IndexColumns[0]);

        auto shardId = CommonFillRecord(ev->Record, shardIdx, buildInfo);
        ev->Record.SetSeed(ui64(shardId));
        LOG_D("TTxBuildProgress: TEvSampleKRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace_back(shardId, ui64(BuildId), std::move(ev));
    }

    void SendBuildIndexRequest(TShardIdx shardIdx, TIndexBuildInfo& buildInfo) {
        auto ev = MakeHolder<TEvDataShard::TEvBuildIndexCreateRequest>();
        ev->Record.SetBuildIndexId(ui64(BuildId));

        ev->Record.SetOwnerId(buildInfo.TablePathId.OwnerId);
        ev->Record.SetPathId(buildInfo.TablePathId.LocalPathId);

        if (buildInfo.IsBuildSecondaryIndex()) {
            if (buildInfo.TargetName.empty()) {
                TPath implTable = TPath::Init(buildInfo.TablePathId, Self).Dive(buildInfo.IndexName).Dive(NTableIndex::ImplTable);
                buildInfo.TargetName = implTable.PathString();

                const auto& implTableInfo = Self->Tables.at(implTable.Base()->PathId);
                auto implTableColumns = NTableIndex::ExtractInfo(implTableInfo);
                buildInfo.FillIndexColumns.clear();
                buildInfo.FillIndexColumns.reserve(implTableColumns.Keys.size());
                for (const auto& x: implTableColumns.Keys) {
                    buildInfo.FillIndexColumns.emplace_back(x);
                    implTableColumns.Columns.erase(x);
                }
                buildInfo.FillDataColumns.clear();
                buildInfo.FillDataColumns.reserve(implTableColumns.Columns.size());
                for (const auto& x: implTableColumns.Columns) {
                    buildInfo.FillDataColumns.emplace_back(x);
                }
            }
            *ev->Record.MutableIndexColumns() = {
                buildInfo.FillIndexColumns.begin(),
                buildInfo.FillIndexColumns.end()
            };
            *ev->Record.MutableDataColumns() = {
                buildInfo.FillDataColumns.begin(),
                buildInfo.FillDataColumns.end()
            };
        } else if (buildInfo.IsBuildColumns()) {
            buildInfo.SerializeToProto(Self, ev->Record.MutableColumnBuildSettings());
        }

        ev->Record.SetTargetName(buildInfo.TargetName);

        ev->Record.SetMaxBatchRows(buildInfo.Limits.MaxBatchRows);
        ev->Record.SetMaxBatchBytes(buildInfo.Limits.MaxBatchBytes);
        ev->Record.SetMaxRetries(buildInfo.Limits.MaxRetries);

        auto shardId = CommonFillRecord(ev->Record, shardIdx, buildInfo);

        LOG_D("TTxBuildProgress: TEvBuildIndexCreateRequest: " << ev->Record.ShortDebugString());

        ToTabletSend.emplace_back(shardId, ui64(BuildId), std::move(ev));
    }
    
    bool SendUploadSampleKRequest(TIndexBuildInfo& buildInfo) {
        if (buildInfo.Sample.Rows.empty()) {
            return false;
        }

        auto implLevelTable = TPath::Init(buildInfo.TablePathId, Self).Dive(buildInfo.IndexName).Dive(NTableIndex::NTableVectorKmeansTreeIndex::LevelTable);
        auto actor = new TUploadSampleK(implLevelTable.PathString(), buildInfo.Limits, Self->SelfId(), ui64(BuildId), std::move(buildInfo.Sample.Rows));

        TActivationContext::AsActorContext().MakeFor(Self->SelfId()).Register(actor);
        return true;
    }

public:
    explicit TTxProgress(TSelf* self, TIndexBuildId id)
        : TTxBase(self, TXTYPE_PROGRESS_INDEX_BUILD)
        , BuildId(id)
    {}

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(BuildId);
        Y_ABORT_UNLESS(buildInfoPtr);
        auto& buildInfo = *buildInfoPtr->Get();

        LOG_I("TTxBuildProgress: Resume"
              << ": id# " << BuildId);
        LOG_D("TTxBuildProgress: Resume"
              << ": " << buildInfo);

        switch (buildInfo.State) {
        case TIndexBuildInfo::EState::Invalid:
            Y_ABORT("Unreachable");

        case TIndexBuildInfo::EState::AlterMainTable:
            if (buildInfo.AlterMainTableTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo.AlterMainTableTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), AlterMainTablePropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.AlterMainTableTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.AlterMainTableTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Locking);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Locking:
            if (buildInfo.LockTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo.LockTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), LockPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.LockTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.LockTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Initiating);
                Progress(BuildId);
            }

            break;
        case TIndexBuildInfo::EState::GatheringStatistics:
            ChangeState(BuildId, TIndexBuildInfo::EState::Initiating);
            Progress(BuildId);
            break;
        case TIndexBuildInfo::EState::Initiating:
            if (buildInfo.InitiateTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo.InitiateTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), InitiatePropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.InitiateTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.InitiateTxId)));
            } else {
                if (buildInfo.IsBuildVectorIndex()) {
                    Y_ASSERT(buildInfo.KMeans.NeedsAnotherLevel());
                    ChangeState(BuildId, TIndexBuildInfo::EState::DropTmp);
                } else {
                    ChangeState(BuildId, TIndexBuildInfo::EState::Filling);
                }

                Progress(BuildId);
            }

            break;
        case TIndexBuildInfo::EState::Filling: {
            auto makeDone = [&] (size_t doneShards) {
                buildInfo.DoneShardsSize = doneShards;
                buildInfo.InProgressShards = {};
                buildInfo.ToUploadShards = {};

                Self->IndexBuildPipes.CloseAll(BuildId, ctx);
            };
            if (buildInfo.IsCancellationRequested()) {
                makeDone(0);

                ChangeState(BuildId, TIndexBuildInfo::EState::Cancellation_Applying);
                Progress(BuildId);

                // make final bill
                Bill(buildInfo);

                break;
            }

            if (buildInfo.Shards.empty()) {
                NIceDb::TNiceDb db(txc.DB);
                InitiateShards(db, buildInfo);
            }

            if (buildInfo.ToUploadShards.empty()
                && buildInfo.DoneShardsSize == 0
                && buildInfo.InProgressShards.empty())
            {
                for (const auto& item: buildInfo.Shards) {
                    const TIndexBuildInfo::TShardStatus& shardStatus = item.second;
                    switch (shardStatus.Status) {
                    case NKikimrIndexBuilder::EBuildStatus::INVALID:
                    case NKikimrIndexBuilder::EBuildStatus::ACCEPTED:
                    case NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS:
                    case NKikimrIndexBuilder::EBuildStatus::ABORTED:
                        buildInfo.ToUploadShards.push_back(item.first);
                        break;
                    case NKikimrIndexBuilder::EBuildStatus::DONE:
                        ++buildInfo.DoneShardsSize;
                        break;
                    case NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR:
                    case NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST:
                        Y_ABORT("Unreachable");
                    }
                }
            }

            if (!buildInfo.SnapshotTxId || !buildInfo.SnapshotStep) {
                Y_ABORT_UNLESS(Self->TablesWithSnapshots.contains(buildInfo.TablePathId));
                Y_ABORT_UNLESS(Self->TablesWithSnapshots.at(buildInfo.TablePathId) == buildInfo.InitiateTxId);

                buildInfo.SnapshotTxId = buildInfo.InitiateTxId;
                Y_ABORT_UNLESS(buildInfo.SnapshotTxId);
                buildInfo.SnapshotStep = Self->SnapshotsStepIds.at(buildInfo.SnapshotTxId);
                Y_ABORT_UNLESS(buildInfo.SnapshotStep);
            }

            if (buildInfo.IsBuildVectorIndex()) {
                buildInfo.Sample.MakeStrictTop(buildInfo.KMeans.K);
                if (buildInfo.Sample.MaxProbability == 0) {
                    makeDone(buildInfo.Shards.size());
                }
            }

            while (!buildInfo.ToUploadShards.empty()
                   && buildInfo.InProgressShards.size() < buildInfo.Limits.MaxShards)
            {
                TShardIdx shardIdx = buildInfo.ToUploadShards.front();
                buildInfo.ToUploadShards.pop_front();
                buildInfo.InProgressShards.insert(shardIdx);

                if (buildInfo.IsBuildVectorIndex()) {
                    SendSampleKRequest(shardIdx, buildInfo);
                } else {
                    SendBuildIndexRequest(shardIdx, buildInfo);
                }
            }


            if (buildInfo.InProgressShards.empty() && buildInfo.ToUploadShards.empty()
                && buildInfo.DoneShardsSize == buildInfo.Shards.size()) {
                // all done
                Y_ABORT_UNLESS(0 == Self->IndexBuildPipes.CloseAll(BuildId, ctx));
                if (buildInfo.IsBuildVectorIndex()) {
                    if (SendUploadSampleKRequest(buildInfo)) {
                        AskToScheduleBilling(buildInfo);
                        break;
                    }
                    if (buildInfo.KMeans.NeedsAnotherLevel()) {
                        AskToScheduleBilling(buildInfo);
                        ChangeState(BuildId, TIndexBuildInfo::EState::DropTmp);
                        Progress(BuildId);
                        break;
                    }
                }
                ChangeState(BuildId, TIndexBuildInfo::EState::Applying);
                Progress(BuildId);

                // make final bill
                Bill(buildInfo);
            } else {
                AskToScheduleBilling(buildInfo);
            }

            break;
        }
        case TIndexBuildInfo::EState::DropTmp:
            Y_ASSERT(buildInfo.IsBuildVectorIndex());
            if (buildInfo.ApplyTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), DropTmpPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.ApplyTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.ApplyTxId)));
            } else {
                buildInfo.ApplyTxId = {};
                buildInfo.ApplyTxStatus = NKikimrScheme::StatusSuccess;
                buildInfo.ApplyTxDone = false;

                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexApplyTxId(db, buildInfo);
                Self->PersistBuildIndexApplyTxStatus(db, buildInfo);
                Self->PersistBuildIndexApplyTxDone(db, buildInfo);

                ChangeState(BuildId, TIndexBuildInfo::EState::CreateTmp);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::CreateTmp:
            Y_ASSERT(buildInfo.IsBuildVectorIndex());
            if (buildInfo.ApplyTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), CreateTmpPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.ApplyTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.ApplyTxId)));
            } else {
                buildInfo.ApplyTxId = {};
                buildInfo.ApplyTxStatus = NKikimrScheme::StatusSuccess;
                buildInfo.ApplyTxDone = false;
                ++buildInfo.KMeans.Level;
                
                NIceDb::TNiceDb db(txc.DB);
                Self->PersistBuildIndexApplyTxId(db, buildInfo);
                Self->PersistBuildIndexApplyTxStatus(db, buildInfo);
                Self->PersistBuildIndexApplyTxDone(db, buildInfo);
                // TODO(mbkkt) persist buildInfo.KMeans.Level

                ChangeState(BuildId, TIndexBuildInfo::EState::Filling);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Applying:
            if (buildInfo.ApplyTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), ApplyPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.ApplyTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.ApplyTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Unlocking);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Unlocking:
            if (buildInfo.UnlockTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo.UnlockTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), UnlockPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.UnlockTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.UnlockTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Done);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Done:
            SendNotificationsIfFinished(buildInfo);
            // stay calm keep status/issues
            break;
        case TIndexBuildInfo::EState::Cancellation_Applying:
            if (buildInfo.ApplyTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), CancelPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.ApplyTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.ApplyTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Cancellation_Unlocking);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
            if (buildInfo.UnlockTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo.UnlockTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), UnlockPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.UnlockTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.UnlockTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Cancelled);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Cancelled:
            SendNotificationsIfFinished(buildInfo);
            // stay calm keep status/issues
            break;
        case TIndexBuildInfo::EState::Rejection_Applying:
            if (buildInfo.ApplyTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo.ApplyTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), CancelPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.ApplyTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.ApplyTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Rejection_Unlocking);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Rejection_Unlocking:
            if (buildInfo.UnlockTxId == InvalidTxId) {
                Send(Self->TxAllocatorClient, MakeHolder<TEvTxAllocatorClient::TEvAllocate>(), 0, ui64(BuildId));
            } else if (buildInfo.UnlockTxStatus == NKikimrScheme::StatusSuccess) {
                Send(Self->SelfId(), UnlockPropose(Self, buildInfo), 0, ui64(BuildId));
            } else if (!buildInfo.UnlockTxDone) {
                Send(Self->SelfId(), MakeHolder<TEvSchemeShard::TEvNotifyTxCompletion>(ui64(buildInfo.UnlockTxId)));
            } else {
                ChangeState(BuildId, TIndexBuildInfo::EState::Rejected);
                Progress(BuildId);
            }
            break;
        case TIndexBuildInfo::EState::Rejected:
            SendNotificationsIfFinished(buildInfo);
            // stay calm keep status/issues
            break;
        }

        return true;
    }

    static TSerializedTableRange InfiniteRange(ui32 columns) {
        TVector<TCell> vec(columns, TCell());
        TArrayRef<TCell> cells(vec);
        return TSerializedTableRange(TSerializedCellVec::Serialize(cells), "", true, false);
    }

    void InitiateShards(NIceDb::TNiceDb& db, TIndexBuildInfo& buildInfo) {
        TTableInfo::TPtr table = Self->Tables.at(buildInfo.TablePathId);

        auto tableColumns = NTableIndex::ExtractInfo(table); // skip dropped columns
        const TSerializedTableRange infiniteRange = InfiniteRange(tableColumns.Keys.size());

        for (const auto& x: table->GetPartitions()) {
            Y_ABORT_UNLESS(Self->ShardInfos.contains(x.ShardIdx));

            buildInfo.Shards.emplace(x.ShardIdx, TIndexBuildInfo::TShardStatus(infiniteRange, ""));
            Self->PersistBuildIndexUploadInitiate(db, buildInfo, x.ShardIdx);
        }
    }

    void DoComplete(const TActorContext& ctx) override {
        for (auto& x: ToTabletSend) {
            Self->IndexBuildPipes.Create(BuildId, std::get<0>(x), std::move(std::get<2>(x)), ctx);
        }
        ToTabletSend.clear();
    }
};

ITransaction* TSchemeShard::CreateTxProgress(TIndexBuildId id) {
    return new TIndexBuilder::TTxProgress(this, id);
}

struct TSchemeShard::TIndexBuilder::TTxBilling: public TSchemeShard::TIndexBuilder::TTxBase  {
private:
    TIndexBuildId BuildIndexId;
    TInstant ScheduledAt;

public:
    explicit TTxBilling(TSelf* self, TEvPrivate::TEvIndexBuildingMakeABill::TPtr& ev)
        : TTxBase(self, TXTYPE_PROGRESS_INDEX_BUILD)
        , BuildIndexId(ev->Get()->BuildId)
        , ScheduledAt(ev->Get()->SendAt)
    {}

    bool DoExecute(TTransactionContext& , const TActorContext& ctx) override {
        LOG_I("TTxReply : TTxBilling"
              << ", buildIndexId# " << BuildIndexId);

        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(BuildIndexId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->Get();

        if (!GotScheduledBilling(buildInfo)) {
            return true;
        }

        Bill(buildInfo, ScheduledAt, ctx.Now());

        AskToScheduleBilling(buildInfo);

        return true;
    }

    void DoComplete(const TActorContext&) override {
    }
};

struct TSchemeShard::TIndexBuilder::TTxReply: public TSchemeShard::TIndexBuilder::TTxBase  {
public:
    explicit TTxReply(TSelf* self)
        : TTxBase(self, TXTYPE_PROGRESS_INDEX_BUILD)
    {}

    void DoComplete(const TActorContext&) override {
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyRetry: public TSchemeShard::TIndexBuilder::TTxReply  {
private:
    struct {
        TIndexBuildId BuildIndexId;
        TTabletId TabletId;
        explicit operator bool() const { return BuildIndexId && TabletId; }
    } PipeRetry;

public:
    explicit TTxReplyRetry(TSelf* self, TIndexBuildId buildId, TTabletId tabletId)
        : TTxReply(self)
        , PipeRetry({buildId, tabletId})
    {}

    bool DoExecute([[maybe_unused]] TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& buildId = PipeRetry.BuildIndexId;
        const auto& tabletId = PipeRetry.TabletId;
        const auto& shardIdx = Self->GetShardIdx(tabletId);

        LOG_I("TTxReply : PipeRetry"
              << ", buildIndexId# " << buildId
              << ", tabletId# " << tabletId
              << ", shardIdx# " << shardIdx);

        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->Get();

        if (!buildInfo.Shards.contains(shardIdx)) {
            return true;
        }

        LOG_D("TTxReply : PipeRetry"
              << ", TIndexBuildInfo: " << buildInfo);

        switch (const auto state = buildInfo.State; state) {
        case TIndexBuildInfo::EState::Filling:
        {
            // reschedule shard
            if (buildInfo.InProgressShards.erase(shardIdx)) {
                buildInfo.ToUploadShards.push_front(shardIdx);

                Self->IndexBuildPipes.Close(buildId, tabletId, ctx);

                // generate new message with actual LastKeyAck to continue scan
                Progress(buildId);
            }
            break;
        }
        case TIndexBuildInfo::EState::AlterMainTable:
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::Locking:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Initiating:
        case TIndexBuildInfo::EState::DropTmp:
        case TIndexBuildInfo::EState::CreateTmp:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << Name(state));
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejection_Applying:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        case TIndexBuildInfo::EState::Rejected:
            LOG_D("TTxReply : PipeRetry"
                  << " superfluous event"
                  << ", buildIndexId# " << buildId
                  << ", tabletId# " << tabletId
                  << ", shardIdx# " << shardIdx);
            break;
        }

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplySampleK: public TSchemeShard::TIndexBuilder::TTxReply  {
private:
    TEvDataShard::TEvSampleKResponse::TPtr SampleK;

public:
    explicit TTxReplySampleK(TSelf* self, TEvDataShard::TEvSampleKResponse::TPtr& sampleK)
        : TTxReply(self)
        , SampleK(sampleK)
    {
    }

    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        auto& record = SampleK->Get()->Record;

        LOG_I("TTxReply : TEvSampleKResponse, Id# " << record.GetId());

        const auto buildId = TIndexBuildId(record.GetId());
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->Get();
        LOG_D("TTxReply : TEvSampleKResponse"
              << ", TIndexBuildInfo: " << buildInfo
              << ", record: " << record.ShortDebugString());

        TTabletId shardId = TTabletId(record.GetTabletId());
        if (!Self->TabletIdToShardIdx.contains(shardId)) {
            return true;
        }

        TShardIdx shardIdx = Self->TabletIdToShardIdx.at(shardId);
        if (!buildInfo.Shards.contains(shardIdx)) {
            return true;
        }

        switch (const auto state = buildInfo.State; state) {
        case TIndexBuildInfo::EState::Filling:
        {
            TIndexBuildInfo::TShardStatus& shardStatus = buildInfo.Shards.at(shardIdx);

            auto actualSeqNo = std::pair<ui64, ui64>(Self->Generation(), shardStatus.SeqNoRound);
            auto recordSeqNo = std::pair<ui64, ui64>(record.GetRequestSeqNoGeneration(), record.GetRequestSeqNoRound());

            if (actualSeqNo != recordSeqNo) {
                LOG_D("TTxReply : TEvBuildIndexProgressResponse"
                      << " ignore progress message by seqNo"
                      << ", TIndexBuildInfo: " << buildInfo
                      << ", actual seqNo for the shard " << shardId << " (" << shardIdx << ") is: "  << Self->Generation() << ":" <<  shardStatus.SeqNoRound
                      << ", record: " << record.ShortDebugString());
                Y_ABORT_UNLESS(actualSeqNo > recordSeqNo);
                return true;
            }

            if (record.ProbabilitiesSize()) {
                Y_ASSERT(record.RowsSize());
                auto& probabilities = record.GetProbabilities();
                auto& rows = *record.MutableRows();
                Y_ASSERT(probabilities.size() == rows.size());
                for (int i = 0; i != probabilities.size(); ++i) {
                    if (probabilities[i] >= buildInfo.Sample.MaxProbability) {
                        break;
                    }
                    buildInfo.Sample.Rows.emplace_back(probabilities[i], std::move(rows[i]));
                }
                buildInfo.Sample.MakeWeakTop(buildInfo.KMeans.K);
            }

            if (record.HasRowsDelta() || record.HasBytesDelta()) {
                TBillingStats delta(record.GetRowsDelta(), record.GetBytesDelta());
                shardStatus.Processed += delta;
                buildInfo.Processed += delta;
            }

            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            shardStatus.DebugMessage = issues.ToString();

            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexUploadProgress(db, buildInfo, shardIdx);
            shardStatus.Status = record.GetStatus();

            switch (shardStatus.Status) {
            case  NKikimrIndexBuilder::EBuildStatus::DONE:
                if (buildInfo.InProgressShards.erase(shardIdx)) {
                    ++buildInfo.DoneShardsSize;

                    Self->IndexBuildPipes.Close(buildId, shardId, ctx);

                    Progress(buildId);
                }
                break;

            case  NKikimrIndexBuilder::EBuildStatus::ABORTED:
                // datashard gracefully rebooted, reschedule shard
                if (buildInfo.InProgressShards.erase(shardIdx)) {
                    buildInfo.ToUploadShards.push_front(shardIdx);

                    Self->IndexBuildPipes.Close(buildId, shardId, ctx);

                    Progress(buildId);
                }
                break;

            case  NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST:
                buildInfo.Issue += TStringBuilder()
                    << "One of the shards report BAD_REQUEST at Filling stage, process has to be canceled"
                    << ", shardId: " << shardId
                    << ", shardIdx: " << shardIdx;
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Rejection_Applying);

                Progress(buildId);
                break;

            case  NKikimrIndexBuilder::EBuildStatus::INVALID:
            case  NKikimrIndexBuilder::EBuildStatus::ACCEPTED:
            case  NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS:
            case  NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR:
                Y_ABORT("Unreachable");
            }
            break;
        }
        case TIndexBuildInfo::EState::AlterMainTable:
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::Locking:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Initiating:
        case TIndexBuildInfo::EState::DropTmp:
        case TIndexBuildInfo::EState::CreateTmp:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << Name(state));
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejection_Applying:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        case TIndexBuildInfo::EState::Rejected:
            LOG_D("TTxReply : TEvSampleKResponse"
                  << " superfluous message " << record.ShortDebugString());
            break;
        }

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyUpload: public TSchemeShard::TIndexBuilder::TTxReply  {
private:
    TEvIndexBuilder::TEvUploadSampleKResponse::TPtr Upload;

public:
    explicit TTxReplyUpload(TSelf* self, TEvIndexBuilder::TEvUploadSampleKResponse::TPtr& upload)
        : TTxReply(self)
        , Upload(upload)
    {
    }

    bool DoExecute([[maybe_unused]] TTransactionContext& txc, [[maybe_unused]] const TActorContext& ctx) override {
        auto& record = Upload->Get()->Record;

        LOG_I("TTxReply : TEvUploadSampleKResponse, Id# " << record.GetId());

        const auto buildId = TIndexBuildId(record.GetId());
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->Get();
        LOG_D("TTxReply : TEvUploadSampleKResponse"
              << ", TIndexBuildInfo: " << buildInfo
              << ", record: " << record.ShortDebugString());
        Y_ASSERT(buildInfo.IsBuildVectorIndex());

        switch (const auto state = buildInfo.State; state) {
        case TIndexBuildInfo::EState::Filling:
        {
            if (record.HasRowsDelta() || record.HasBytesDelta()) {
                TBillingStats delta(record.GetRowsDelta(), record.GetBytesDelta());
                buildInfo.Processed += delta;
            }

            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);

            NIceDb::TNiceDb db(txc.DB);
            auto status = record.GetUploadStatus();
            if (status == Ydb::StatusIds::SUCCESS) {
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Filling);
                Progress(buildId);
            } else {
                NYql::TIssues issues;
                NYql::IssuesFromMessage(record.GetIssues(), issues);
                buildInfo.Issue += issues.ToString();
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Rejection_Applying);
                Progress(buildId);
            }
            break;
        }
        case TIndexBuildInfo::EState::AlterMainTable:
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::Locking:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Initiating:
        case TIndexBuildInfo::EState::DropTmp:
        case TIndexBuildInfo::EState::CreateTmp:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << Name(state));
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejection_Applying:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        case TIndexBuildInfo::EState::Rejected:
            LOG_D("TTxReply : TEvUploadSampleKResponse superfluous message " << record.ShortDebugString());
            break;
        }

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyProgress: public TSchemeShard::TIndexBuilder::TTxReply  {
private:
    TEvDataShard::TEvBuildIndexProgressResponse::TPtr ShardProgress;
public:
    explicit TTxReplyProgress(TSelf* self, TEvDataShard::TEvBuildIndexProgressResponse::TPtr& shardProgress)
        : TTxReply(self)
        , ShardProgress(shardProgress)
    {}


    bool DoExecute(TTransactionContext& txc, const TActorContext& ctx) override {
        const NKikimrTxDataShard::TEvBuildIndexProgressResponse& record = ShardProgress->Get()->Record;

        LOG_I("TTxReply : TEvBuildIndexProgressResponse"
              << ", buildIndexId# " << record.GetBuildIndexId());

        const auto buildId = TIndexBuildId(record.GetBuildIndexId());
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        if (!buildInfoPtr) {
            return true;
        }
        auto& buildInfo = *buildInfoPtr->Get();
        LOG_D("TTxReply : TEvBuildIndexProgressResponse"
              << ", TIndexBuildInfo: " << buildInfo
              << ", record: " << record.ShortDebugString());

        TTabletId shardId = TTabletId(record.GetTabletId());
        if (!Self->TabletIdToShardIdx.contains(shardId)) {
            return true;
        }

        TShardIdx shardIdx = Self->TabletIdToShardIdx.at(shardId);
        if (!buildInfo.Shards.contains(shardIdx)) {
            return true;
        }

        switch (const auto state = buildInfo.State; state) {
        case TIndexBuildInfo::EState::Filling:
        {
            TIndexBuildInfo::TShardStatus& shardStatus = buildInfo.Shards.at(shardIdx);

            auto actualSeqNo = std::pair<ui64, ui64>(Self->Generation(), shardStatus.SeqNoRound);
            auto recordSeqNo = std::pair<ui64, ui64>(record.GetRequestSeqNoGeneration(), record.GetRequestSeqNoRound());

            if (actualSeqNo != recordSeqNo) {
                LOG_D("TTxReply : TEvBuildIndexProgressResponse"
                      << " ignore progress message by seqNo"
                      << ", TIndexBuildInfo: " << buildInfo
                      << ", actual seqNo for the shard " << shardId << " (" << shardIdx << ") is: "  << Self->Generation() << ":" <<  shardStatus.SeqNoRound
                      << ", record: " << record.ShortDebugString());
                Y_ABORT_UNLESS(actualSeqNo > recordSeqNo);
                return true;
            }

            if (record.HasLastKeyAck()) {
                if (shardStatus.LastKeyAck) {
                    //check that all LastKeyAcks are monotonously increase
                    TTableInfo::TPtr tableInfo = Self->Tables.at(buildInfo.TablePathId);
                    TVector<NScheme::TTypeInfo> keyTypes;
                    for (ui32 keyPos: tableInfo->KeyColumnIds) {
                        keyTypes.push_back(tableInfo->Columns.at(keyPos).PType);
                    }

                    TSerializedCellVec last;
                    last.Parse(shardStatus.LastKeyAck);

                    TSerializedCellVec update;
                    update.Parse(record.GetLastKeyAck());

                    int cmp = CompareBorders<true, true>(last.GetCells(),
                                                         update.GetCells(),
                                                         true,
                                                         true,
                                                         keyTypes);
                    Y_VERIFY_S(cmp < 0,
                               "check that all LastKeyAcks are monotonously increase"
                                   << ", last: " << DebugPrintPoint(keyTypes, last.GetCells(), *AppData()->TypeRegistry)
                                   << ", update: " <<  DebugPrintPoint(keyTypes, update.GetCells(), *AppData()->TypeRegistry));
                }

                shardStatus.LastKeyAck = record.GetLastKeyAck();
            }

            if (record.HasRowsDelta() || record.HasBytesDelta()) {
                TBillingStats delta(record.GetRowsDelta(), record.GetBytesDelta());
                shardStatus.Processed += delta;
                buildInfo.Processed += delta;
            }

            shardStatus.Status = record.GetStatus();
            shardStatus.UploadStatus = record.GetUploadStatus();
            NYql::TIssues issues;
            NYql::IssuesFromMessage(record.GetIssues(), issues);
            shardStatus.DebugMessage = issues.ToString();

            NIceDb::TNiceDb db(txc.DB);
            Self->PersistBuildIndexUploadProgress(db, buildInfo, shardIdx);

            switch (shardStatus.Status) {
            case  NKikimrIndexBuilder::EBuildStatus::INVALID:
                Y_ABORT("Unreachable");

            case  NKikimrIndexBuilder::EBuildStatus::ACCEPTED:
            case  NKikimrIndexBuilder::EBuildStatus::IN_PROGRESS:
                // no oop, wait resolution. Progress key are persisted
                break;

            case  NKikimrIndexBuilder::EBuildStatus::DONE:
                if (buildInfo.InProgressShards.erase(shardIdx)) {
                    ++buildInfo.DoneShardsSize;

                    Self->IndexBuildPipes.Close(buildId, shardId, ctx);

                    Progress(buildId);
                }
                break;

            case  NKikimrIndexBuilder::EBuildStatus::ABORTED:
                // datashard gracefully rebooted, reschedule shard
                if (buildInfo.InProgressShards.erase(shardIdx)) {
                    buildInfo.ToUploadShards.push_front(shardIdx);

                    Self->IndexBuildPipes.Close(buildId, shardId, ctx);

                    Progress(buildId);
                }
                break;

            case  NKikimrIndexBuilder::EBuildStatus::BUILD_ERROR:
                buildInfo.Issue += TStringBuilder()
                    << "One of the shards report BUILD_ERROR at Filling stage, process has to be canceled"
                    << ", shardId: " << shardId
                    << ", shardIdx: " << shardIdx;
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Rejection_Applying);

                Progress(buildId);
                break;
            case  NKikimrIndexBuilder::EBuildStatus::BAD_REQUEST:
                buildInfo.Issue += TStringBuilder()
                    << "One of the shards report BAD_REQUEST at Filling stage, process has to be canceled"
                    << ", shardId: " << shardId
                    << ", shardIdx: " << shardIdx;
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo.Id, TIndexBuildInfo::EState::Rejection_Applying);

                Progress(buildId);
                break;
            }

            break;
        }
        case TIndexBuildInfo::EState::AlterMainTable:
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::Locking:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Initiating:
        case TIndexBuildInfo::EState::DropTmp:
        case TIndexBuildInfo::EState::CreateTmp:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Done:
            Y_FAIL_S("Unreachable " << Name(state));
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejection_Applying:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        case TIndexBuildInfo::EState::Rejected:
            LOG_D("TTxReply : TEvBuildIndexProgressResponse"
                  << " superfluous message " << record.ShortDebugString());
            break;
        }

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyCompleted: public TSchemeShard::TIndexBuilder::TTxReply  {
private:
    TTxId CompletedTxId;
public:
    explicit TTxReplyCompleted(TSelf* self, TTxId completedTxId)
        : TTxReply(self)
        , CompletedTxId(completedTxId)
    {}
 
    bool DoExecute(TTransactionContext& txc, [[maybe_unused]] const TActorContext& ctx) override {
        const auto txId = CompletedTxId;
        const auto* buildIdPtr = Self->TxIdToIndexBuilds.FindPtr(txId);
        if (!buildIdPtr) {
            LOG_I("TTxReply : TEvNotifyTxCompletionResult superfluous message"
                  << ", txId: " << txId
                  << ", buildInfoId not found");
            return true;
        }

        const auto buildId = *buildIdPtr;
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        Y_ABORT_UNLESS(buildInfoPtr);
        auto& buildInfo = *buildInfoPtr->Get();
        LOG_I("TTxReply : TEvNotifyTxCompletionResult"
              << ", txId# " << txId
              << ", buildInfoId: " << buildInfo.Id);
        LOG_D("TTxReply : TEvNotifyTxCompletionResult"
              << ", txId# " << txId
              << ", buildInfo: " << buildInfo);

        const auto state = buildInfo.State;
        NIceDb::TNiceDb db(txc.DB);

        switch (state) {
        case TIndexBuildInfo::EState::AlterMainTable:
        {
            Y_ABORT_UNLESS(txId == buildInfo.AlterMainTableTxId);

            buildInfo.AlterMainTableTxDone = true;
            Self->PersistBuildIndexAlterMainTableTxDone(db, buildInfo);
            break;
        }
        case TIndexBuildInfo::EState::Locking:
        {
            Y_ABORT_UNLESS(txId == buildInfo.LockTxId);

            buildInfo.LockTxDone = true;
            Self->PersistBuildIndexLockTxDone(db, buildInfo);
            break;
        }
        case TIndexBuildInfo::EState::Initiating:
        {
            Y_ABORT_UNLESS(txId == buildInfo.InitiateTxId);

            buildInfo.InitiateTxDone = true;
            Self->PersistBuildIndexInitiateTxDone(db, buildInfo);
            break;
        }
        case TIndexBuildInfo::EState::DropTmp:
        case TIndexBuildInfo::EState::CreateTmp:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Rejection_Applying:
        {
            Y_VERIFY_S(txId == buildInfo.ApplyTxId, state);

            buildInfo.ApplyTxDone = true;
            Self->PersistBuildIndexApplyTxDone(db, buildInfo);
            break;
        }
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        {
            Y_VERIFY_S(txId == buildInfo.UnlockTxId, state);

            buildInfo.UnlockTxDone = true;
            Self->PersistBuildIndexUnlockTxDone(db, buildInfo);
            break;
        }
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Filling:
        case TIndexBuildInfo::EState::Done:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejected:
            Y_FAIL_S("Unreachable " << Name(state));
        }

        Progress(buildId);

        return true;
    }

};

struct TSchemeShard::TIndexBuilder::TTxReplyModify: public TSchemeShard::TIndexBuilder::TTxReply  {
private:
    TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr ModifyResult;
public:
    explicit TTxReplyModify(TSelf* self, TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& modifyResult)
        : TTxReply(self)
        , ModifyResult(modifyResult)
    {}

    void ReplyOnCreation(const TIndexBuildInfo& buildInfo,
                         const Ydb::StatusIds::StatusCode status = Ydb::StatusIds::SUCCESS) {
        auto responseEv = MakeHolder<TEvIndexBuilder::TEvCreateResponse>(ui64(buildInfo.Id));

        Fill(*responseEv->Record.MutableIndexBuild(), buildInfo);

        auto& response = responseEv->Record;
        response.SetStatus(status);

        if (buildInfo.Issue) {
            AddIssue(response.MutableIssues(), buildInfo.Issue);
        }

        LOG_N("TIndexBuilder::TTxReply: ReplyOnCreation"
              << ", BuildIndexId: " << buildInfo.Id
              << ", status: " << Ydb::StatusIds::StatusCode_Name(status)
              << ", error: " << buildInfo.Issue
              << ", replyTo: " << buildInfo.CreateSender.ToString());
        LOG_D("Message:\n" << responseEv->Record.ShortDebugString());

        Send(buildInfo.CreateSender, std::move(responseEv), 0, buildInfo.SenderCookie);
    }

    bool DoExecute(TTransactionContext& txc, [[maybe_unused]] const TActorContext& ctx) override {
        const auto& record = ModifyResult->Get()->Record;

        const auto txId = TTxId(record.GetTxId());
        const auto* buildIdPtr = Self->TxIdToIndexBuilds.FindPtr(txId);
        if (!buildIdPtr) {
            LOG_I("TTxReply : TEvModifySchemeTransactionResult superfluous message"
                  << ", cookie: " << ModifyResult->Cookie
                  << ", txId: " << record.GetTxId()
                  << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                  << ", BuildIndexId not found");
            return true;
        }

        const auto buildId = *buildIdPtr;
        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        Y_ABORT_UNLESS(buildInfoPtr);
        // We need this because we use buildInfo after EraseBuildInfo
        auto buildInfoPin = *buildInfoPtr;
        auto& buildInfo = *buildInfoPin;

        LOG_I("TTxReply : TEvModifySchemeTransactionResult"
              << ", BuildIndexId: " << buildId
              << ", cookie: " << ModifyResult->Cookie
              << ", txId: " << record.GetTxId()
              << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus()));

        LOG_D("TTxReply : TEvModifySchemeTransactionResult"
              << ", buildInfo: " << buildInfo
              << ", record: " << record.ShortDebugString());

        const auto state = buildInfo.State;
        NIceDb::TNiceDb db(txc.DB);

        auto replyOnCreation = [&] {
            auto statusCode = TranslateStatusCode(record.GetStatus());

            if (statusCode != Ydb::StatusIds::SUCCESS) {
                buildInfo.Issue += TStringBuilder()
                    << "At " << Name(state) << " state got unsuccess propose result"
                    << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                    << ", reason: " << record.GetReason();
                Self->PersistBuildIndexIssue(db, buildInfo);
                Self->PersistBuildIndexForget(db, buildInfo);
                EraseBuildInfo(buildInfo);
            }

            ReplyOnCreation(buildInfo, statusCode);
        };

        auto ifErrorMoveTo = [&] (TIndexBuildInfo::EState to) {
            if (record.GetStatus() == NKikimrScheme::StatusAccepted) {
                // no op
            } else if (record.GetStatus() == NKikimrScheme::StatusAlreadyExists) {
                Y_ABORT("NEED MORE TESTING");
                // no op
            } else {
                buildInfo.Issue += TStringBuilder()
                    << "At " << Name(state) << " state got unsuccess propose result"
                    << ", status: " << NKikimrScheme::EStatus_Name(record.GetStatus())
                    << ", reason: " << record.GetReason();
                Self->PersistBuildIndexIssue(db, buildInfo);
                ChangeState(buildInfo.Id, to);
            }
        };

        switch (state) {
        case TIndexBuildInfo::EState::AlterMainTable:
        {
            Y_ABORT_UNLESS(txId == buildInfo.AlterMainTableTxId);

            buildInfo.AlterMainTableTxStatus = record.GetStatus();
            Self->PersistBuildIndexAlterMainTableTxStatus(db, buildInfo);

            replyOnCreation();
            break;
        }
        case TIndexBuildInfo::EState::Locking:
        {
            Y_ABORT_UNLESS(txId == buildInfo.LockTxId);

            buildInfo.LockTxStatus = record.GetStatus();
            Self->PersistBuildIndexLockTxStatus(db, buildInfo);

            replyOnCreation();
            break;
        }
        case TIndexBuildInfo::EState::Initiating:
        {
            Y_ABORT_UNLESS(txId == buildInfo.InitiateTxId);

            buildInfo.InitiateTxStatus = record.GetStatus();
            Self->PersistBuildIndexInitiateTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_Unlocking);
            break;
        }
        case TIndexBuildInfo::EState::DropTmp:
        case TIndexBuildInfo::EState::CreateTmp:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Rejection_Applying:
        {
            Y_ABORT_UNLESS(txId == buildInfo.ApplyTxId);

            buildInfo.ApplyTxStatus = record.GetStatus();
            Self->PersistBuildIndexApplyTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_Unlocking);
            break;
        }
        case TIndexBuildInfo::EState::Unlocking:
        {
            Y_ABORT_UNLESS(txId == buildInfo.UnlockTxId);

            buildInfo.UnlockTxStatus = record.GetStatus();
            Self->PersistBuildIndexUnlockTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Rejection_Unlocking);
            break;
        }
        case TIndexBuildInfo::EState::Cancellation_Applying:
        {
            Y_ABORT_UNLESS(txId == buildInfo.ApplyTxId);

            buildInfo.ApplyTxStatus = record.GetStatus();
            Self->PersistBuildIndexApplyTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Cancellation_Unlocking);
            break;
        }
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        {
            Y_ABORT_UNLESS(txId == buildInfo.UnlockTxId);

            buildInfo.UnlockTxStatus = record.GetStatus();
            Self->PersistBuildIndexUnlockTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Cancelled);
            break;
        }
        case TIndexBuildInfo::EState::Rejection_Unlocking:
        {
            Y_ABORT_UNLESS(txId == buildInfo.UnlockTxId);

            buildInfo.UnlockTxStatus = record.GetStatus();
            Self->PersistBuildIndexUnlockTxStatus(db, buildInfo);

            ifErrorMoveTo(TIndexBuildInfo::EState::Rejected);
            break;
        }
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Filling:
        case TIndexBuildInfo::EState::Done:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejected:
            Y_FAIL_S("Unreachable " << Name(state));
        }

        Progress(buildId);

        return true;
    }
};

struct TSchemeShard::TIndexBuilder::TTxReplyAllocate: public TSchemeShard::TIndexBuilder::TTxReply  {
private:
    TEvTxAllocatorClient::TEvAllocateResult::TPtr AllocateResult;
public:
    explicit TTxReplyAllocate(TSelf* self, TEvTxAllocatorClient::TEvAllocateResult::TPtr& allocateResult)
        : TTxReply(self)
        , AllocateResult(allocateResult)
    {}

    bool DoExecute(TTransactionContext& txc,[[maybe_unused]] const TActorContext& ctx) override {
        TIndexBuildId buildId = TIndexBuildId(AllocateResult->Cookie);
        const auto txId = TTxId(AllocateResult->Get()->TxIds.front());

        LOG_I("TTxReply : TEvAllocateResult"
              << ", BuildIndexId: " << buildId
              << ", txId# " << txId);

        const auto* buildInfoPtr = Self->IndexBuilds.FindPtr(buildId);
        Y_ABORT_UNLESS(buildInfoPtr);
        auto& buildInfo = *buildInfoPtr->Get();

        LOG_D("TTxReply : TEvAllocateResult"
              << ", buildInfo: " << buildInfo);

        NIceDb::TNiceDb db(txc.DB);
        const auto state = buildInfo.State;

        switch (state) {
        case TIndexBuildInfo::EState::AlterMainTable:
            if (!buildInfo.AlterMainTableTxId) {
                buildInfo.AlterMainTableTxId = txId;
                Self->PersistBuildIndexAlterMainTableTxId(db, buildInfo);
            }
            break;
        case TIndexBuildInfo::EState::Locking:
            if (!buildInfo.LockTxId) {
                buildInfo.LockTxId = txId;
                Self->PersistBuildIndexLockTxId(db, buildInfo);
            }
            break;
        case TIndexBuildInfo::EState::Initiating:
            if (!buildInfo.InitiateTxId) {
                buildInfo.InitiateTxId = txId;
                Self->PersistBuildIndexInitiateTxId(db, buildInfo);
            }
            break;
        case TIndexBuildInfo::EState::DropTmp:
        case TIndexBuildInfo::EState::CreateTmp:
        case TIndexBuildInfo::EState::Applying:
        case TIndexBuildInfo::EState::Cancellation_Applying:
        case TIndexBuildInfo::EState::Rejection_Applying:
            if (!buildInfo.ApplyTxId) {
                buildInfo.ApplyTxId = txId;
                Self->PersistBuildIndexApplyTxId(db, buildInfo);
            }
            break;
        case TIndexBuildInfo::EState::Unlocking:
        case TIndexBuildInfo::EState::Cancellation_Unlocking:
        case TIndexBuildInfo::EState::Rejection_Unlocking:
            if (!buildInfo.UnlockTxId) {
                buildInfo.UnlockTxId = txId;
                Self->PersistBuildIndexUnlockTxId(db, buildInfo);
            }
            break;
        case TIndexBuildInfo::EState::Invalid:
        case TIndexBuildInfo::EState::GatheringStatistics:
        case TIndexBuildInfo::EState::Filling:
        case TIndexBuildInfo::EState::Done:
        case TIndexBuildInfo::EState::Cancelled:
        case TIndexBuildInfo::EState::Rejected:
            Y_FAIL_S("Unreachable " << Name(state));
        }

        Progress(buildId);

        return true;
    }
};

ITransaction* TSchemeShard::CreateTxReply(TEvTxAllocatorClient::TEvAllocateResult::TPtr& allocateResult) {
    return new TIndexBuilder::TTxReplyAllocate(this, allocateResult);
}

ITransaction* TSchemeShard::CreateTxReply(TEvSchemeShard::TEvModifySchemeTransactionResult::TPtr& modifyResult) {
    return new TIndexBuilder::TTxReplyModify(this, modifyResult);
}

ITransaction* TSchemeShard::CreateTxReply(TTxId completedTxId) {
    return new TIndexBuilder::TTxReplyCompleted(this, completedTxId);
}

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvBuildIndexProgressResponse::TPtr& progress) {
    return new TIndexBuilder::TTxReplyProgress(this, progress);
}

ITransaction* TSchemeShard::CreateTxReply(TEvDataShard::TEvSampleKResponse::TPtr& sampleK) {
    return new TIndexBuilder::TTxReplySampleK(this, sampleK);
}

ITransaction* TSchemeShard::CreateTxReply(TEvIndexBuilder::TEvUploadSampleKResponse::TPtr& upload) {
    return new TIndexBuilder::TTxReplyUpload(this, upload);
}

ITransaction* TSchemeShard::CreatePipeRetry(TIndexBuildId indexBuildId, TTabletId tabletId) {
    return new TIndexBuilder::TTxReplyRetry(this, indexBuildId, tabletId);
}

ITransaction* TSchemeShard::CreateTxBilling(TEvPrivate::TEvIndexBuildingMakeABill::TPtr& ev) {
    return new TIndexBuilder::TTxBilling(this, ev);
}


} // NSchemeShard
} // NKikimr
