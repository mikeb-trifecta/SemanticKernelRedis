using Microsoft.SemanticKernel.AI.Embeddings;
using Microsoft.SemanticKernel.Memory;
using StackExchange.Redis;
using NRedisStack;
using NRedisStack.Search;
using NRedisStack.RedisStackCommands;
using NRedisStack.Search.Literals.Enums;
using Newtonsoft.Json;


namespace Trifecta.SemanticKernelRedis
{
    internal class RedisMemoryStore : IMemoryStore
    {
        ConnectionMultiplexer _redis;
        IDatabase _store;

        ISearchCommandsAsync _search;

        IJsonCommandsAsync _json;

        private Schema _schema;
        
        public RedisMemoryStore(ConfigurationOptions configuration)
        {
            _redis = ConnectionMultiplexer.Connect(configuration);
            BuildStackReferences();
        }

        public RedisMemoryStore(string config)
        {
            _redis = ConnectionMultiplexer.Connect(config);
            BuildStackReferences();
        }

        private void BuildStackReferences()
        {
            _store = _redis.GetDatabase();
            _search = _store.FT();
            _json =  _store.JSON();
        }

        public Task CreateCollectionAsync(string collectionName, CancellationToken cancel = default)
        {

            Console.WriteLine("CreateCollectionAsync");
            var vectorFieldDef = new Dictionary<string, object>();
            vectorFieldDef["TYPE"] = "FLOAT32";
            vectorFieldDef["DIM"] = 1536;
            vectorFieldDef["DISTANCE_METRIC"] = "COSINE";

            _schema = new Schema()
                .AddTextField( new FieldName("$.key", "key"))
                .AddTextField(new FieldName("$.metadata", "metadata"))
                .AddVectorField(new FieldName("$.embedding", "embedding"), Schema.VectorField.VectorAlgo.HNSW, vectorFieldDef);

            return _search.CreateAsync(collectionName, new FTCreateParams().On(IndexDataType.JSON), _schema);

        }

        public Task DeleteCollectionAsync(string collectionName, CancellationToken cancel = default)
        {
            Console.WriteLine("DeleteCollectionAsync");
            throw new NotImplementedException();
        }

        public async Task<bool> DoesCollectionExistAsync(string collectionName, CancellationToken cancel = default)
        {
            Console.WriteLine("DoesCollectionExistAsync");

            var indexes = await _search._ListAsync();

            foreach (var index in indexes) { 
                Console.WriteLine(index.ToString());
                if (collectionName == index.ToString()) return true;
            }

            return false;
        }

        public async Task<MemoryRecord> GetAsync(string collectionName, string key, bool withEmbedding = false, CancellationToken cancel = default)
        {
            Console.WriteLine("GetAsync");

            //var val = _store.StringGet(key);
            var fields = new[] { "$.key", "$.metadata" };
            if (withEmbedding) fields.Append("$.embedding");

            var res = (string) await _json.GetAsync(key: key, paths: fields);
            if (res == null) return JsonConvert.DeserializeObject<MemoryRecord>("");
            return JsonConvert.DeserializeObject<MemoryRecord>(res);
        }

        public IAsyncEnumerable<MemoryRecord> GetBatchAsync(string collectionName, IEnumerable<string> keys, bool withEmbeddings = false, CancellationToken cancel = default)
        {
            Console.WriteLine("GetBatchAsync");
            throw new NotImplementedException();
        }

        public IAsyncEnumerable<string> GetCollectionsAsync(CancellationToken cancel = default)
        {
            Console.WriteLine("GetCollectionsAsync");
            return (IAsyncEnumerable<string>) _search._ListAsync();
        }

        public async Task<(MemoryRecord, double)?> GetNearestMatchAsync(string collectionName, Embedding<float> embedding, double minRelevanceScore = 0, bool withEmbedding = false, CancellationToken cancel = default)
        {
            return await this
                .GetNearestMatchesAsync(collectionName, embedding, 1, minRelevanceScore, withEmbedding, cancel)
                .FirstOrDefaultAsync(cancellationToken: cancel);
        }

        public async IAsyncEnumerable<(MemoryRecord, double)> GetNearestMatchesAsync(string collectionName, Embedding<float> embedding, int limit, double minRelevanceScore = 0, bool withEmbeddings = false, CancellationToken cancel = default)
        {
            byte[] query_vector = embedding.Vector.SelectMany(BitConverter.GetBytes).ToArray();

            var query = new Query($"*=>[KNN {limit} @embedding $query_vec]")
                .AddParam("query_vec", query_vector)
                .SetSortBy("__vector_score")
                .ReturnFields(new FieldName("$.metadata", "metadata"))
                .Dialect(2);

            var results = await _search.SearchAsync(collectionName, query);
            foreach (var doc in results.Documents.Where(x => x.Score > minRelevanceScore)) {

                var mem = MemoryRecord.FromJsonMetadata(doc["metadata"], null);

                yield return new(mem, (double) doc["__vector_score"]);
            }
        }

        public Task RemoveAsync(string collectionName, string key, CancellationToken cancel = default)
        {
            return _json.DelAsync(key);
        }

        public async Task RemoveBatchAsync(string collectionName, IEnumerable<string> keys, CancellationToken cancel = default)
        {
            foreach (var key in keys)
            {
                await RemoveAsync(collectionName, key, cancel);
            }
        }

        public async Task<string> UpsertAsync(string collectionName, MemoryRecord record, CancellationToken cancel = default)
        {
            Console.WriteLine("UpsertAsync");
            var t = await _json.SetAsync(record.Key, "$", record);

            return t.ToString();
        }

        public async IAsyncEnumerable<string> UpsertBatchAsync(string collectionName, IEnumerable<MemoryRecord> records, CancellationToken cancel = default)
        {
            foreach (var r in records)
            {
                yield return await UpsertAsync(collectionName, r, cancel);
            }
        }
    }
}