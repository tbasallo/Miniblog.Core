using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using System.Xml.Linq;
using System.Xml.XPath;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Configuration;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Miniblog.Core.Models;
using Newtonsoft.Json;

namespace Miniblog.Core.Services
{
    public class AzureTableStorageBlogService : IBlogService
    {
        private readonly List<Post> _cache = new List<Post>();
        private readonly IHttpContextAccessor _contextAccessor;        
        private readonly CloudTable _cloudTable;

        public AzureTableStorageBlogService(IConfiguration config, IHttpContextAccessor contextAccessor)
        {
            _contextAccessor = contextAccessor;

            var tableName = config["AzureTableStorage:TableName"] ?? "MiniblogCoreServicesPosts";

            var storageAccount = CloudStorageAccount.Parse(config.GetConnectionString("AzureStorageAccount"));
            var cloudTableClient = storageAccount.CreateCloudTableClient();
            _cloudTable = cloudTableClient.GetTableReference(tableName);

            //This should be a flag somewhere to skip this step in every instantiation, perhaps in StartUp - once per APP - if even there
            //also it can address the having to wait piece
            _cloudTable.CreateIfNotExistsAsync().Wait();

            Initialize();
        }

        public virtual Task<IEnumerable<Post>> GetPosts(int count, int skip = 0)
        {
            bool isAdmin = IsAdmin();

            var posts = _cache
                .Where(p => p.PubDate <= DateTime.UtcNow && (p.IsPublished || isAdmin))
                .Skip(skip)
                .Take(count);

            return Task.FromResult(posts);
        }

        public virtual Task<IEnumerable<Post>> GetPostsByCategory(string category)
        {
            bool isAdmin = IsAdmin();

            var posts = from p in _cache
                        where p.PubDate <= DateTime.UtcNow && (p.IsPublished || isAdmin)
                        where p.Categories.Contains(category, StringComparer.OrdinalIgnoreCase)
                        select p;

            return Task.FromResult(posts);

        }

        public virtual Task<Post> GetPostBySlug(string slug)
        {
            var post = _cache.FirstOrDefault(p => p.Slug.Equals(slug, StringComparison.OrdinalIgnoreCase));
            bool isAdmin = IsAdmin();

            if (post != null && post.PubDate <= DateTime.UtcNow && (post.IsPublished || isAdmin))
            {
                return Task.FromResult(post);
            }

            return Task.FromResult<Post>(null);
        }

        public virtual Task<Post> GetPostById(string id)
        {
            var post = _cache.FirstOrDefault(p => p.ID.Equals(id, StringComparison.OrdinalIgnoreCase));
            bool isAdmin = IsAdmin();

            if (post != null && post.PubDate <= DateTime.UtcNow && (post.IsPublished || isAdmin))
            {
                return Task.FromResult(post);
            }

            return Task.FromResult<Post>(null);
        }

        public virtual Task<IEnumerable<string>> GetCategories()
        {
            bool isAdmin = IsAdmin();

            var categories = _cache
                .Where(p => p.IsPublished || isAdmin)
                .SelectMany(post => post.Categories)
                .Select(cat => cat.ToLowerInvariant())
                .Distinct();

            return Task.FromResult(categories);
        }

        public async Task SavePost(Post post)
        {            
            post.LastModified = DateTime.UtcNow;

            var entity = SetPost(post);            
            var operation = TableOperation.InsertOrReplace(entity);
            await _cloudTable.ExecuteAsync(operation);            

            if (!_cache.Contains(post))
            {
                _cache.Add(post);
                SortCache();
            }
        }

        public async Task DeletePost(Post post)
        {
            var entity = SetPost(post);
            var operation = TableOperation.Delete(entity);
            await _cloudTable.ExecuteAsync(operation);
            
            if (_cache.Contains(post))
            {
                _cache.Remove(post);
            }            
        }

        public async Task<string> SaveFile(byte[] bytes, string fileName, string suffix = null)
        {
            await Task.Delay(0); //I know, I just hate the warnings. This only until we figure out what to do here            
            throw new NotImplementedException();
        }

        private void Initialize()
        {
            //only doing Wait() until I know if we can move this somewhere else that better supports an Async initializer
            LoadPostsAsync().Wait();
            SortCache();
        }

        async Task LoadPostsAsync()
        {
            TableQuery<TableStoragePost> query = new TableQuery<TableStoragePost>();
            TableContinuationToken tableContinuationToken = null;
            
            var entities = await _cloudTable.ExecuteQuerySegmentedAsync(query, tableContinuationToken);
            
            tableContinuationToken = AddPosts(entities);
            //as of this writing it takes at least 1000 entities to get here
            while (tableContinuationToken != null)
            {
                entities = await _cloudTable.ExecuteQuerySegmentedAsync(query, tableContinuationToken);
                AddPosts(entities);
                tableContinuationToken = AddPosts(entities);
            }
        }

        TableContinuationToken AddPosts(TableQuerySegment<TableStoragePost> entities)
        {
            if (entities.Results != null)
            {
                entities.Results.ToList().ForEach(p =>
                {
                    _cache.Add(GetPost(p));
                });

                return entities.ContinuationToken;
            }

            return null;
        }

        protected void SortCache()
        {
            _cache.Sort((p1, p2) => p2.PubDate.CompareTo(p1.PubDate));
        }

        protected bool IsAdmin()
        {
            return _contextAccessor.HttpContext?.User?.Identity.IsAuthenticated == true;
        }


        Post GetPost(TableStoragePost post)
        {
            var p = new Post
            {
                ID = post.ID,
                Title = post.Title,
                Slug = post.Slug,
                Excerpt = post.Excerpt,
                Content = post.Content,
                PubDate = post.PubDate,
                LastModified = post.LastModified,
                IsPublished = post.IsPublished,
                Categories = JsonConvert.DeserializeObject<List<string>>(post.Categories)
            };

            //Need to double check, but it seems that this is not needed since Categories may never be null
            //if (!string.IsNullOrWhiteSpace(post.Categories))
            //{
            //    var categories = JsonConvert.DeserializeObject<List<string>>(post.Categories);
            //    p.Categories = categories;
            //}

            if (!string.IsNullOrWhiteSpace(post.Comments))
            {
                var comments = JsonConvert.DeserializeObject<List<Comment>>(post.Comments);
                if (comments != null && comments.Count > 0)
                {
                    foreach (var comment in comments)
                    {
                        p.Comments.Add(comment);
                    }
                }
            }

            return p;
        }
        TableStoragePost SetPost(Post post)
        {
            //is there any better options for partition and row key? I'm not quite sure
            var p = new TableStoragePost
            {
                PartitionKey = post.ID,
                RowKey = post.ID,
                ID = post.ID,
                Title = post.Title,
                Slug = post.Slug,
                Excerpt = post.Excerpt,
                Content = post.Content,
                PubDate = post.PubDate,
                LastModified = post.LastModified,
                IsPublished = post.IsPublished,
                Categories = JsonConvert.SerializeObject(post.Categories),
                Comments = JsonConvert.SerializeObject(post.Comments),
            };

            return p;
        }

        class TableStoragePost : TableEntity
        {
            public string ID { get; set; } = DateTime.UtcNow.Ticks.ToString();
            public string Title { get; set; }
            public string Slug { get; set; }
            public string Excerpt { get; set; }
            public string Content { get; set; }
            public DateTime PubDate { get; set; } = DateTime.UtcNow;
            public DateTime LastModified { get; set; } = DateTime.UtcNow;
            public bool IsPublished { get; set; } = true;
            public string Categories { get; set; }
            public string Comments { get; set; }
        }

    }
}
