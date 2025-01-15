#include <stdio.h>
#include <time.h>
#include <string.h>
#include <assert.h>
#include <stdint.h>
#include <algorithm>
#include <vector>

//======================== Perfect hash generator =========================
 
typedef uint32_t hash_int;
 
//searching for a hash function of this kind:
hash_int HashFunction(hash_int param, hash_int x, int bits) {
    return (param * x) >> hash_int(32 - bits);
}
 
static const hash_int NO_PARAM = hash_int(-1);
hash_int GeneratePerfectHash(std::vector<hash_int> values, int bits, hash_int tries = NO_PARAM) {
    std::vector<int> table;
    for (hash_int param = 0; param < tries; param++) {
        table.assign(hash_int(1)<<bits, 0);
        bool ok = true;
        for (int i = 0; ok && i < values.size(); i++) {
            hash_int x = values[i];
            hash_int cell = HashFunction(param, x, bits);
            if (table[cell]++)
                ok = false;
        }
        if (ok)
            return param;
    }
    return NO_PARAM;
}

//======================== Lookup table generator =========================
 
template<class T> void PrintTable(const T *lut, int size) {
    for (int i = 0; i < size; i++) {
        const unsigned char *entry = (unsigned char*) &lut[i];
        printf("\t{");
        for (int j = 0; j < sizeof(T); j++) {
            if (j) printf(", ");
            printf("0x%02X", (unsigned int)(entry[j]));
        }
        printf("},\n");
    }
    printf("\n");
}


struct m256d {
    int d[8];
};
static const int HASH_BITS = 7;
m256d lookupTable[1<<HASH_BITS];

uint32_t GenerateLookupTableHash() {
    std::vector<uint32_t> allMasks;
    std::vector<m256d> allPerms;
    for (int m = 0; m < 1<<8; m++) {
        int ak = 0;
        int a[8], b[8];
        m256d perm;
        for (int i = 0; i < 8; i++) {
            if (m & (1<<i)) {
                perm.d[i] = ak;
                a[ak++] = i;
            }
            else {
                perm.d[i] = 4 + (i-ak);
                b[i-ak] = i;
            }
        }
        if (ak != 4) continue;
        
        int mask = 0;
        for (int s = 0; s < 4; s++)
            for (int i = 0; i < 4; i++) {
                int bit = (a[i] < b[(i+s) % 4]);
                mask ^= (bit << (s*4+i));
            }

        allMasks.push_back(mask);
        allPerms.push_back(perm);
    }

    uint32_t param = GeneratePerfectHash(allMasks, HASH_BITS);
    assert(param != NO_PARAM);

    memset(lookupTable, -1, sizeof(lookupTable));
    for (int i = 0; i < allMasks.size(); i++) {
        uint32_t hash = HashFunction(param, allMasks[i], HASH_BITS);
        lookupTable[hash] = allPerms[i];
    }
    return param;
}

int main() {
    uint32_t param = GenerateLookupTableHash();
    printf("lookupTable (%08X):\n", param);
    PrintTable(lookupTable, 1 << HASH_BITS);
    return 0;
}