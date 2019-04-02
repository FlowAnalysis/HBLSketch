#ifndef _ELASTIC_SKETCH_H_
#define _ELASTIC_SKETCH_H_

#include "HeavyPart.h"
#include "BufferLayer.h"
#include "LightPart.h"

template<int bucket_num, int tot_memory_in_bytes, int Bbucket_num>
class ElasticSketch
{
    static constexpr int heavy_mem = bucket_num * COUNTER_PER_BUCKET * 8;
    static constexpr int buffer_mem = Bbucket_num * 8;
    static constexpr int light_mem = tot_memory_in_bytes - heavy_mem - buffer_mem;

    HeavyPart<bucket_num> heavy_part;
    BufferLayer<Bbucket_num> buffer_layer;
    LightPart<light_mem> light_part;

    //printf("bbucket_num: %d\n", bucket_num);
    //printf("llight_mem: %d\n", light_mem);

public:
    ElasticSketch(){}
    ~ElasticSketch(){}
    void clear()
    {
        heavy_part.clear();
        buffer_layer.clear();
        light_part.clear();
    }

    void insert(uint8_t *key, int f = 1)
    {
        //printf("key=%d\n",*key);
        uint8_t swap_key[KEY_LENGTH_4];
        uint32_t swap_val = 0;
        uint8_t Bswap_key[KEY_LENGTH_4];
        uint32_t Bswap_val = 0;
        int result = heavy_part.insert(key, swap_key, swap_val, f);

        switch(result)
        {
            case 0: return;
            case 1:{
                int resultB = buffer_layer.insert(swap_key, Bswap_key, Bswap_val, swap_val);
                if(resultB==1)
                {
                    if(HIGHEST_BIT_IS_1(Bswap_val))
                    {
                        light_part.insert(Bswap_key, GetCounterVal(Bswap_val));
                    }
                    else
                    {
                        light_part.swap_insert(Bswap_key, Bswap_val);
                    }
                }                    
                return;
            }
            case 2:{
                int resultB = buffer_layer.insert(key, Bswap_key, Bswap_val, 1);  
                if(resultB==1)
                {
                    if(HIGHEST_BIT_IS_1(Bswap_val))
                    {
                        light_part.insert(Bswap_key, GetCounterVal(Bswap_val));
                    }
                    else
                    {
                        light_part.swap_insert(Bswap_key, Bswap_val);
                    }
                }
                return;
            } 
            default:
                printf("error return value !\n");
                exit(1);
        }
    }

    void quick_insert(uint8_t *key, int f = 1)
    {
        heavy_part.quick_insert(key, f);
    }

    int query(uint8_t *key)
    {
        uint32_t heavy_result = heavy_part.query(key);
        if(heavy_result == 0 || HIGHEST_BIT_IS_1(heavy_result))
        {
            uint32_t buffer_result = buffer_layer.query(key);
            //if(heavy_result == 0 || HIGHEST_BIT_IS_1(heavy_result))

            int light_result = light_part.query(key);
            return (int)GetCounterVal(heavy_result) + (int)GetCounterVal(buffer_result) + light_result;
        }
        return heavy_result;
    }

    int query_compressed_part(uint8_t *key, uint8_t *compress_part, int compress_counter_num)
    {
        uint32_t heavy_result = heavy_part.query(key);
        if(heavy_result == 0 || HIGHEST_BIT_IS_1(heavy_result))
        {
            int light_result = light_part.query_compressed_part(key, compress_part, compress_counter_num);
            return (int)GetCounterVal(heavy_result) + light_result;
        }
        return heavy_result;
    }

    void get_heavy_hitters(int threshold, vector<pair<string, int>> & results)
    {
        for (int i = 0; i < bucket_num; ++i) 
            for (int j = 0; j < MAX_VALID_COUNTER; ++j) 
            {
                ////printf("<%x>", heavy_part.buckets[i].key[j]);
                uint32_t key = heavy_part.buckets[i].key[j];
                int val = query((uint8_t *)&key);
                if (val >= threshold) {
                    results.push_back(make_pair(string((const char*)&key, 4), val));
                }
            }
    }

/* interface */
    int get_compress_width(int ratio) { return light_part.get_compress_width(ratio);}
    void compress(int ratio, uint8_t *dst) {    light_part.compress(ratio, dst); }
    int get_bucket_num() { return heavy_part.get_bucket_num(); }
    double get_bandwidth(int compress_ratio) 
    {
        int result = heavy_part.get_memory_usage();
        result += get_compress_width(compress_ratio) * sizeof(uint8_t);
        return result * 1.0 / 1024 / 1024;
    }

    int get_cardinality()
    {
        int card = light_part.get_cardinality();
        for(int i = 0; i < bucket_num; ++i)
            for(int j = 0; j < MAX_VALID_COUNTER; ++j)
            {
                uint8_t key[KEY_LENGTH_4];
                *(uint32_t*)key = heavy_part.buckets[i].key[j];
                int val = heavy_part.buckets[i].val[j];
                int ex_val = light_part.query(key);

                if(HIGHEST_BIT_IS_1(val) && ex_val)
                {
                    val += ex_val;
                    card--;
                }
                if(GetCounterVal(val))
                    card++;
            }
            return card;
    }

    double get_entropy()
    {
        int tot = 0;
        double entr = 0;

        light_part.get_entropy(tot, entr);

        for(int i = 0; i < bucket_num; ++i)
            for(int j = 0; j < MAX_VALID_COUNTER; ++j)
            {
                uint8_t key[KEY_LENGTH_4];
                *(uint32_t*)key = heavy_part.buckets[i].key[j];
                int val = heavy_part.buckets[i].val[j];

                int ex_val = light_part.query(key);

                if(HIGHEST_BIT_IS_1(val) && ex_val)
                {
                    val += ex_val;

                    tot -= ex_val;

                    entr -= ex_val * log2(ex_val);
                }
                val = GetCounterVal(val);
                if(val)
                {
                    tot += val;
                    entr += val * log2(val);
                }
            }
        return -entr / tot + log2(tot);
    }

    void get_distribution(vector<double> &dist)
    {
        light_part.get_distribution(dist);

        for(int i = 0; i < bucket_num; ++i)
            for(int j = 0; j < MAX_VALID_COUNTER; ++j)
            {
                uint8_t key[KEY_LENGTH_4];
                *(uint32_t*)key = heavy_part.buckets[i].key[j];
                int val = heavy_part.buckets[i].val[j];

                int ex_val = light_part.query(key);

                if(HIGHEST_BIT_IS_1(val) && ex_val != 0)
                {
                    val += ex_val;
                    dist[ex_val]--;
                }
                val = GetCounterVal(val);
                if(val)
                {
                    if(val + 1 > dist.size())
                        dist.resize(val + 1);
                    dist[val]++;
                }
            }
    }

    void *operator new(size_t sz)
    {
        constexpr uint32_t alignment = 64;
        size_t alloc_size = (2 * alignment + sz) / alignment * alignment;
        void *ptr = ::operator new(alloc_size);
        void *old_ptr = ptr;
        void *new_ptr = ((char*)std::align(alignment, sz, ptr, alloc_size) + alignment);
        ((void **)new_ptr)[-1] = old_ptr;

        return new_ptr;
    }
    void operator delete(void *p)
    {
        ::operator delete(((void**)p)[-1]);
    }
};



#endif
