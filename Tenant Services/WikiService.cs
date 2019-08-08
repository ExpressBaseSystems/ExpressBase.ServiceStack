using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
using ServiceStack.Redis;
using System.Linq;
using System.Threading.Tasks;
using System.Text;

namespace ExpressBase.ServiceStack.Services
{
    public class WikiService : EbBaseTenatService
    {
        public WikiService()
        { }

        public PersistWikiResponse Post(PersistWikiRequest request)
        {
            PersistWikiResponse resp = new PersistWikiResponse()
            {
                Wiki = request.Wiki
            };
            try
            {
                string query = @"
            INSERT INTO 
            wiki (
                    category, title, html, eb_created_at, eb_created_by, eb_tags , status , wiki_category_id) 
            VALUES (
                    @category, @title, @html, @createdon, @createdby, @tags, @status, @catid)
            RETURNING id";

                DbParameter[] parameters = new DbParameter[]
                {
                this.InfraConnectionFactory.DataDB.GetNewParameter("category", EbDbTypes.String, request.Wiki.Category),
                this.InfraConnectionFactory.DataDB.GetNewParameter("title", EbDbTypes.String, request.Wiki.Title),
                this.InfraConnectionFactory.DataDB.GetNewParameter("html", EbDbTypes.String, request.Wiki.HTML),
                 this.InfraConnectionFactory.DataDB.GetNewParameter("createdon", EbDbTypes.DateTime, DateTime.Now),
                this.InfraConnectionFactory.DataDB.GetNewParameter("createdby", EbDbTypes.Int32, request.Wiki.CreatedBy),
                this.InfraConnectionFactory.DataDB.GetNewParameter("tags", EbDbTypes.String, request.Wiki.Tags),
                this.InfraConnectionFactory.DataDB.GetNewParameter("status", EbDbTypes.String, request.Wiki.Status),
                this.InfraConnectionFactory.DataDB.GetNewParameter("catid", EbDbTypes.Int32, request.Wiki.CatId)
                };

                EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(query, parameters);

                resp.Wiki.Id = (int)dt.Rows[0][0];

                resp.ResponseStatus = true;
                request.Wiki.Id = resp.Wiki.Id;
                var hashfield = Encoding.UTF8.GetBytes(resp.Wiki.Id.ToString());
                var hashval = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(resp.Wiki));
                var Temp = (this.Redis as RedisClient).HSet("wiki", hashfield, hashval); 
            }
            catch (Exception e)
            {
                resp.ResponseStatus = false;
            }

            return resp;
        }

        public UpdateWikiResponse Post(UpdateWikiRequest request)
        {
            UpdateWikiResponse resp = new UpdateWikiResponse()
            {
                Wiki = request.Wiki
            };
            try
            {
                string query = @"
            UPDATE wiki SET
                category= @category, title = @title , html = @html , eb_lastmodified_by = @modified_by, eb_lastmodified_at = @updatedtime, eb_tags = @tags, status =@status, wiki_category_id = @catid
            WHERE 
                id= @id
            RETURNING id";

                DbParameter[] parameters = new DbParameter[]
                {
                this.InfraConnectionFactory.DataDB.GetNewParameter("category", EbDbTypes.String, request.Wiki.Category),
                this.InfraConnectionFactory.DataDB.GetNewParameter("title", EbDbTypes.String, request.Wiki.Title),
                this.InfraConnectionFactory.DataDB.GetNewParameter("html", EbDbTypes.String, request.Wiki.HTML),
                this.InfraConnectionFactory.DataDB.GetNewParameter("modified_by", EbDbTypes.Int32, request.Wiki.CreatedBy),
                this.InfraConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Wiki.Id),
                 this.InfraConnectionFactory.DataDB.GetNewParameter("tags", EbDbTypes.String, request.Wiki.Tags),
                 this.InfraConnectionFactory.DataDB.GetNewParameter("status", EbDbTypes.String, request.Wiki.Status),
                 this.InfraConnectionFactory.DataDB.GetNewParameter("updatedtime", EbDbTypes.DateTime, DateTime.Now),
                 this.InfraConnectionFactory.DataDB.GetNewParameter("catid", EbDbTypes.Int32, request.Wiki.CatId)
                };

                EbDataTable x = InfraConnectionFactory.DataDB.DoQuery(query, parameters);

                resp.Wiki.Id = (int)x.Rows[0][0];
                request.Wiki.Id = resp.Wiki.Id;
                var hashfield = Encoding.UTF8.GetBytes(resp.Wiki.Id.ToString());
                var hashval = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(resp.Wiki));
                var Temp = (this.Redis as RedisClient).HSet("wiki", hashfield, hashval);
                resp.ResponseStatus = true;
            }
            catch (Exception e)
            {
                resp.ResponseStatus = false;
            }

            return resp;
        }

        public GetWikiByIdResponse Get(GetWikiByIdRequest request)
        {
            GetWikiByIdResponse resp = new GetWikiByIdResponse();

            resp.Wiki = new Wiki();
            try
            {

                DbParameter[] parameters = new DbParameter[]
                {
                this.InfraConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Id)
                };

                string query = @"
                 SELECT *
                 FROM
                    wiki  
                 WHERE
                   id = @id AND eb_del='F' ; 
                SELECT * FROM wiki_category WHERE status='publish' ; ";

                EbDataSet ds = InfraConnectionFactory.DataDB.DoQueries(query, parameters);

                resp.Wiki.Category = ds.Tables[0].Rows[0]["category"].ToString();
                resp.Wiki.Title = ds.Tables[0].Rows[0]["title"].ToString();
                resp.Wiki.HTML = ds.Tables[0].Rows[0]["html"].ToString();
                resp.Wiki.Tags = ds.Tables[0].Rows[0]["eb_tags"].ToString();
                resp.Wiki.Status = ds.Tables[0].Rows[0]["status"].ToString();
                resp.Wiki.CatId = (int)ds.Tables[0].Rows[0]["wiki_category_id"];
                int capacity1 = ds.Tables[1].Rows.Count;
                Console.WriteLine("INFO: Wiki Count: " + capacity1);
                for (int i = 0; i < capacity1; i++)
                {
                    resp.WikiCat.Add(
                        new WikiCat()
                        {
                            WikiCategory = ds.Tables[1].Rows[i]["category"].ToString(),
                            WikiCatId = (int)ds.Tables[1].Rows[i]["id"]
                        });

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: Inside GetWikiByIdRequest: " + e.Message + e.StackTrace);
                return null;
            }
            return resp;
        }


        public AddNewWikiResponse Get(AddNewWikiRequest request)
        {
            AddNewWikiResponse resp = new AddNewWikiResponse();
            try
            {
                string query = @" 
                SELECT * FROM wiki_category WHERE status='publish'";
                EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(query);

                int capacity1 = dt.Rows.Count;
                Console.WriteLine("INFO: Wiki Count: " + capacity1);
                for (int i = 0; i < capacity1; i++)
                {
                    resp.WikiCat.Add(
                        new WikiCat()
                        {
                            WikiCategory = dt.Rows[i]["category"].ToString(),
                            WikiIconClass = dt.Rows[i]["icon_class"].ToString(),
                            WikiCatId = (int) dt.Rows[i]["id"]
                        });

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: GetWikiList Exception: " + e.Message);
            }
            return resp;
      }



        public GetWikiBySearchResponse Get(GetWikiBySearchRequest request)
        {
            GetWikiBySearchResponse resp = new GetWikiBySearchResponse();
            try
            {
                DbParameter[] parameters = new DbParameter[]
                    {
                this.InfraConnectionFactory.DataDB.GetNewParameter("search_wiki", EbDbTypes.String, request.Wiki_Search)
                    };
              string query = @"
                SELECT *
                FROM
                    wiki
                WHERE status='Publish' AND
                    ( title LIKE '%' || @search_wiki || '%' OR  eb_tags LIKE '%' || @search_wiki || '%' ) ORDER BY list_order";

                EbDataTable table = InfraConnectionFactory.DataDB.DoQuery(query, parameters);

                int capacity = table.Rows.Count;

                for (int i = 0; i < capacity; i++)
                {
                    resp.WikiListBySearch.Add(
                        new Wiki()
                        {
                            Category = table.Rows[i]["category"].ToString(),
                            HTML = table.Rows[i]["html"].ToString(),
                            Title = table.Rows[i]["title"].ToString(),
                            Id = (int)table.Rows[i]["id"],
                            Tags = table.Rows[i]["eb_tags"].ToString()
                        });
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: GetWikiSearch Exception: " + e.Message);
            }
            return resp;
        }



        public GetWikiListResponse Get(GetWikiListRequest request)
        {
            GetWikiListResponse resp = new GetWikiListResponse();

            try
            {

                string query = @"
                SELECT *
                FROM
                    wiki 
                WHERE 
                    eb_del='F' AND status='Publish' ORDER BY list_order ; 
                SELECT * FROM wiki_category WHERE status='publish'; ";
                EbDataSet ds = InfraConnectionFactory.DataDB.DoQueries(query);

                int capacity = ds.Tables[0].Rows.Count;

                Console.WriteLine("INFO: Wiki Count: " + capacity);

                for (int i = 0; i < capacity; i++)
                {
                    resp.WikiList.Add(
                        new Wiki()
                        {
                            Category = ds.Tables[0].Rows[i]["category"].ToString(),
                            HTML = ds.Tables[0].Rows[i]["html"].ToString(),
                            Title = ds.Tables[0].Rows[i]["title"].ToString(),
                            Id = (int)ds.Tables[0].Rows[i]["id"],
                            Order = (int)ds.Tables[0].Rows[i]["list_order"],
                            CatId = (int)ds.Tables[0].Rows[i]["wiki_category_id"]
                        });
                }
                int capacity1 = ds.Tables[1].Rows.Count;
                Console.WriteLine("INFO: Wiki Count: " + capacity1);
                for (int i = 0; i < capacity1; i++)
                {
                    resp.WikiCat.Add(
                        new WikiCat()
                        {
                            WikiCategory = ds.Tables[1].Rows[i]["category"].ToString(),
                            WikiIconClass = ds.Tables[1].Rows[i]["icon_class"].ToString(),
                            WikiDescription = ds.Tables[1].Rows[i]["description"].ToString(),
                            WikiCatId = (int) ds.Tables[1].Rows[i]["id"]
                        });

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: GetWikiList Exception: " + e.Message);
            }
            return resp;
        }



        public GetWikiResponse Get(GetWikiRequest request)
        {
            GetWikiResponse resp = new GetWikiResponse();

            resp.Wiki = new Wiki();
            try
            {

                var hashfield = Encoding.UTF8.GetBytes(request.Id.ToString());
                var tem2 = (this.Redis as RedisClient).HGet("wiki", hashfield);
                if(tem2 != null)
                {
                    string my_string = Encoding.UTF8.GetString(tem2);
                    var abc = JsonConvert.SerializeObject(resp.Wiki);
                    Wiki obj = JsonConvert.DeserializeObject<Wiki>(my_string);

                    resp.Wiki.Category = obj.Category.ToString();
                    resp.Wiki.Title = obj.Title.ToString();
                    resp.Wiki.HTML = obj.HTML.ToString();
                    resp.Wiki.Tags = obj.Tags.ToString();
                }

                else
                {
                    DbParameter[] parameters = new DbParameter[]
                   {
                this.InfraConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Id)
                   };
                    string query = @"
                 SELECT *
                 FROM
                    wiki  
                 WHERE
                    id = @id AND eb_del='F'  ORDER BY list_order";

                    EbDataTable table = InfraConnectionFactory.DataDB.DoQuery(query, parameters);

                    resp.Wiki.Category = table.Rows[0]["category"].ToString();
                    resp.Wiki.Title = table.Rows[0]["title"].ToString();
                    resp.Wiki.HTML = table.Rows[0]["html"].ToString();
                    resp.Wiki.Tags = table.Rows[0]["eb_tags"].ToString();
                    resp.Wiki.Id = request.Id;

                    var hashval = Encoding.UTF8.GetBytes(JsonConvert.SerializeObject(resp.Wiki));
                    var Temp = (this.Redis as RedisClient).HSet("wiki", hashfield, hashval);

                }

               

            }
            catch (Exception e)
            {
                Console.Write("ERROR in PublicWiki/GetWiki" + e.Message + e.StackTrace);
            }
            return resp;
        }

        //public WikiAdminResponse Get(WikiAdminRequest request)
        //{
        //    WikiAdminResponse resp = new WikiAdminResponse();

        //    try
        //    {

        //        string query = @"
        //        SELECT *
        //        FROM
        //            wiki 
        //        WHERE 
        //            eb_del='F' ORDER BY id  ";
        //        EbDataTable table = InfraConnectionFactory.DataDB.DoQuery(query);

        //        int count = table.Rows.Count;

        //        Console.WriteLine("INFO: Wiki Count: " + count);

        //        for (int i = 0; i < count; i++)
        //        {
        //            resp.WikiList.Add(
        //                new Wiki()
        //                {
        //                    Category = table.Rows[i]["category"].ToString(),
        //                    HTML = table.Rows[i]["html"].ToString(),
        //                    Title = table.Rows[i]["title"].ToString(),
        //                    Id = (int)table.Rows[i]["id"],
        //                    Status = table.Rows[i]["status"].ToString()
        //                });
        //        }
        //    }
        //    catch (Exception e)
        //    {
        //        Console.WriteLine("ERROR: GetWikiList Exception: " + e.Message + e.StackTrace);
        //    }
        //    return resp;
        //}



        public Admin_Wiki_ListResponse Get(Admin_Wiki_ListRequest request)
        {
            Admin_Wiki_ListResponse resp = new Admin_Wiki_ListResponse();
            try
            {
                DbParameter[] parameters = new DbParameter[]
                    {
                this.InfraConnectionFactory.DataDB.GetNewParameter("status", EbDbTypes.String, request.Status)
                    };
                string query = @"
                 SELECT *
                 FROM
                    wiki  
                 WHERE
                    status = @status AND eb_del='F'; 
                 SELECT * FROM wiki_category WHERE status='publish'; ";
                EbDataSet ds = InfraConnectionFactory.DataDB.DoQueries(query , parameters);

                int capacity = ds.Tables[0].Rows.Count;

                Console.WriteLine("INFO: Wiki Count: " + capacity);

                for (int i = 0; i < capacity; i++)
                {
                    resp.WikiList.Add(
                        new Wiki()
                        {
                            Category = ds.Tables[0].Rows[i]["category"].ToString(),
                            HTML = ds.Tables[0].Rows[i]["html"].ToString(),
                            Title = ds.Tables[0].Rows[i]["title"].ToString(),
                            Id = (int)ds.Tables[0].Rows[i]["id"],
                            Order = (int)ds.Tables[0].Rows[i]["list_order"],
                            Status = ds.Tables[0].Rows[i]["status"].ToString(),
                            CreatedAt = ((DateTime)ds.Tables[0].Rows[i]["eb_created_at"]).Date

                        });
                }
                int capacity1 = ds.Tables[1].Rows.Count;
                Console.WriteLine("INFO: Wiki Count: " + capacity1);
                for (int i = 0; i < capacity1; i++)
                {
                    resp.WikiCat.Add(
                        new WikiCat()
                        {
                            WikiCategory = ds.Tables[1].Rows[i]["category"].ToString(), 
                        });

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: GetWikiList Exception: " + e.Message);
            }
            return resp;
        }



        public Publish_wikiResponse Post(Publish_wikiRequest request)
        {
            Publish_wikiResponse resp = new Publish_wikiResponse()
            {
                Id = request.Wiki_id
            };

            try
            {
                string query = @"
            UPDATE wiki SET
                status =@status
            WHERE 
                id= @id
            RETURNING id";

                DbParameter[] parameters = new DbParameter[]
                {
                this.InfraConnectionFactory.DataDB.GetNewParameter("status", EbDbTypes.String,request.Status),
                this.InfraConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Wiki_id),

                };

                EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(query, parameters);

                resp.Id = (int)dt.Rows[0][0];

            }
            catch (Exception e)
            {

            }

            return resp;
        }

        public PublicViewResponse Get(PublicViewRequest request)
        {
            PublicViewResponse resp = new PublicViewResponse();

            try
            {

                string query = @"
                SELECT *
                FROM
                    wiki 
                WHERE 
                    eb_del='F' AND status='Publish' order by list_order ; 
                SELECT* FROM wiki_category WHERE status = 'publish';";
                EbDataSet ds = InfraConnectionFactory.DataDB.DoQueries(query);

                int capacity = ds.Tables[0].Rows.Count;

                Console.WriteLine("INFO: Wiki Count: " + capacity);

                for (int i = 0; i < capacity; i++)
                {
                    resp.WikiList.Add(
                        new Wiki()
                        {
                            Category = ds.Tables[0].Rows[i]["category"].ToString(),
                            HTML = ds.Tables[0].Rows[i]["html"].ToString(),
                            Title = ds.Tables[0].Rows[i]["title"].ToString(),
                            Id = (int)ds.Tables[0].Rows[i]["id"],
                            Order = (int)ds.Tables[0].Rows[i]["list_order"],
                            Status = ds.Tables[0].Rows[i]["status"].ToString()
                        });
                }
                int capacity1 = ds.Tables[1].Rows.Count;
                Console.WriteLine("INFO: Wiki Count: " + capacity1);
                for (int i = 0; i < capacity1; i++)
                {
                    resp.WikiCat.Add(
                        new WikiCat()
                        {
                            WikiCategory = ds.Tables[1].Rows[i]["category"].ToString(),
                        });

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: GetWikiList Exception: " + e.Message);
            }
            return resp;
        }


        public UpdateOrderResponse Post(UpdateOrderRequest request)
        {
            UpdateOrderResponse resp = new UpdateOrderResponse();


            try
            {
                //    string query = @"
                //UPDATE wiki SET
                //     list_order = @list_order
                //WHERE 
                //    id = @id ";
                List<int> arr = JsonConvert.DeserializeObject<List<int>>(request.Wiki_id);
                List<DbParameter> param = new List<DbParameter>();
                List<string> str = new List<string>();
                for (int i = 0; i < arr.Count; i++)
                {
                    param.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("id_" + i, EbDbTypes.Int32, arr[i]));
                    param.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("list_order_" + i, EbDbTypes.Int32, i + 1));
                    str.Add(string.Format("(@id_{0}, @list_order_{0})", i));
                }
                string query1 = string.Format(@" update wiki as w 
                set list_order = c.list_order
                from (values {0}) as c(id, list_order) 
                where c.id = w.id;", string.Join(",", str));


                int x = InfraConnectionFactory.DataDB.DoNonQuery(query1, param.ToArray());

                resp.ResponseStatus = x > 0;
            }
            catch (Exception e)
            {
                resp.ResponseStatus = false;
            }

            return resp;
        }

        public FileRefByContextResponse Get(FileRefByContextRequest request)
        {
            string Qry = @"
                            SELECT 
	                            B.id, B.filename, B.tags, B.uploadts
                            FROM
	                            eb_files_ref B
                            WHERE
	                            B.context = :context";

            FileRefByContextResponse resp = new FileRefByContextResponse();
            try
            {
                DbParameter[] param = new DbParameter[]
                {
                this.InfraConnectionFactory.DataDB.GetNewParameter("context", EbDbTypes.String, request.Context)
                };

                var dt = this.InfraConnectionFactory.DataDB.DoQuery(Qry, param);

                foreach (EbDataRow dr in dt.Rows)
                {
                    FileMetaInfo info = new FileMetaInfo
                    {
                        FileRefId = Convert.ToInt32(dr["id"]),
                        FileName = dr["filename"] as string,
                        Meta = JsonConvert.DeserializeObject<Dictionary<string, List<string>>>(dr["tags"] as string),
                        UploadTime = Convert.ToDateTime(dr["uploadts"]).ToString("dd-MM-yyyy hh:mm tt")
                    };

                    if (!resp.Images.Contains(info))
                        resp.Images.Add(info);
                }
            }
            catch (Exception e)
            {

            }
            return resp;
        }

    }
}

