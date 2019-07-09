using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

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
                    category, title, html, eb_created_at, eb_created_by, eb_tags , status) 
            VALUES (
                    @category, @title, @html, @createdon, @createdby, @tags, @status)
            RETURNING id";

                DbParameter[] parameters = new DbParameter[]
                {
                this.InfraConnectionFactory.DataDB.GetNewParameter("category", EbDbTypes.String, request.Wiki.Category),
                this.InfraConnectionFactory.DataDB.GetNewParameter("title", EbDbTypes.String, request.Wiki.Title),
                this.InfraConnectionFactory.DataDB.GetNewParameter("html", EbDbTypes.String, request.Wiki.HTML),
                 this.InfraConnectionFactory.DataDB.GetNewParameter("createdon", EbDbTypes.DateTime, DateTime.Now),
                this.InfraConnectionFactory.DataDB.GetNewParameter("createdby", EbDbTypes.Int32, request.Wiki.CreatedBy),
                this.InfraConnectionFactory.DataDB.GetNewParameter("tags", EbDbTypes.String, request.Wiki.Tags),
                this.InfraConnectionFactory.DataDB.GetNewParameter("status", EbDbTypes.String, request.Wiki.Status)
                };

                EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(query, parameters);

                resp.Wiki.Id = (int)dt.Rows[0][0];

                resp.ResponseStatus = true;
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
                category= @category, title = @title , html = @html , eb_last_modified_by = @modified_by, eb_last_modified_at = @updatedtime, eb_tags = @tags, status =@status
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
                 this.InfraConnectionFactory.DataDB.GetNewParameter("updatedtime", EbDbTypes.DateTime, DateTime.Now)
                };

                EbDataTable x = InfraConnectionFactory.DataDB.DoQuery(query, parameters);

                resp.Wiki.Id = (int)x.Rows[0][0];

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
                   id = @id AND eb_del='false'  ";

                EbDataTable table = InfraConnectionFactory.DataDB.DoQuery(query, parameters);

                resp.Wiki.Category = table.Rows[0]["category"].ToString();
                resp.Wiki.Title = table.Rows[0]["title"].ToString();
                resp.Wiki.HTML = table.Rows[0]["html"].ToString();
                resp.Wiki.Tags = table.Rows[0]["eb_tags"].ToString();


            }
            catch (Exception e)
            {
                Console.WriteLine("Exception inside Getting Wiki By Id");
                return null;
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
                WHERE
                    title LIKE '%' || @search_wiki || '%' OR  eb_tags LIKE '%' || @search_wiki || '%' And status='Publish' ORDER BY list_order";

                EbDataTable table = InfraConnectionFactory.DataDB.DoQuery(query, parameters);

                int capacity = table.Rows.Capacity;

                for (int i = 0; i < capacity; i++)
                {
                    resp.WikiListBySearch.Add(
                        new Wiki()
                        {
                            Category = table.Rows[i]["category"].ToString(),
                            HTML = table.Rows[i]["html"].ToString(),
                            Title = table.Rows[i]["title"].ToString(),
                            Id = (int)table.Rows[i]["id"]
                        });
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: GetWikiList Exception: " + e.Message);
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
                    eb_del='false' AND status='Publish' ORDER BY list_order  ";
                EbDataTable table = InfraConnectionFactory.DataDB.DoQuery(query);

                int capacity = table.Rows.Capacity;

                for (int i = 0; i < capacity; i++)
                {
                    resp.WikiList.Add(
                        new Wiki()
                        {
                            Category = table.Rows[i]["category"].ToString(),
                            HTML = table.Rows[i]["html"].ToString(),
                            Title = table.Rows[i]["title"].ToString(),
                            Id = (int) table.Rows[i]["id"]  
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
                DbParameter[] parameters = new DbParameter[]
                    {
                this.InfraConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Id)
                    };
                string query = @"
                 SELECT *
                 FROM
                    wiki  
                 WHERE
                    id = @id AND eb_del='false'  ORDER BY list_order";

                EbDataTable table = InfraConnectionFactory.DataDB.DoQuery(query, parameters);

                resp.Wiki.Category = table.Rows[0]["category"].ToString();
                resp.Wiki.Title = table.Rows[0]["title"].ToString();
                resp.Wiki.HTML = table.Rows[0]["html"].ToString();
                resp.Wiki.Tags = table.Rows[0]["eb_tags"].ToString();

            }
            catch (Exception e)
            {
                Console.Write("exception in PublicWiki/GetWiki");
            }
            return resp;
        }

        public WikiAdminResponse Get(WikiAdminRequest request)
        {
            WikiAdminResponse resp = new WikiAdminResponse();

            try
            {

                string query = @"
                SELECT *
                FROM
                    wiki 
                WHERE 
                    eb_del='false' ORDER BY id  ";
                EbDataTable table = InfraConnectionFactory.DataDB.DoQuery(query);

                int capacity = table.Rows.Capacity;

                for (int i = 0; i < capacity; i++)
                {
                    resp.WikiList.Add(
                        new Wiki()
                        {
                            Category = table.Rows[i]["category"].ToString(),
                            HTML = table.Rows[i]["html"].ToString(),
                            Title = table.Rows[i]["title"].ToString(),
                            Id = (int)table.Rows[i]["id"],
                            Status = table.Rows[i]["status"].ToString()
                        });
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("ERROR: GetWikiList Exception: " + e.Message);
            }
            return resp;
        }



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
                    status = @status AND eb_del='false' ";

                EbDataTable table = InfraConnectionFactory.DataDB.DoQuery(query, parameters);

                int capacity = table.Rows.Count;

                for (int i = 0; i < capacity; i++)
                {
                    resp.WikiList.Add(
                        new Wiki()
                        {
                            Category = table.Rows[i]["category"].ToString(),
                            HTML = table.Rows[i]["html"].ToString(),
                            Title = table.Rows[i]["title"].ToString(),
                            Id = (int)table.Rows[i]["id"],
                            Status = table.Rows[i]["status"].ToString(),
                            CreatedAt = (DateTime)table.Rows[i]["eb_created_at"],
                           

                        });
                }
            }
            catch (Exception e)
            {

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
                    eb_del='false' AND status='Publish' order by list_order ";
                EbDataTable table = InfraConnectionFactory.DataDB.DoQuery(query);

                int capacity = table.Rows.Capacity;

                for (int i = 0; i < capacity; i++)
                {
                    resp.WikiList.Add(
                        new Wiki()
                        {
                            Category = table.Rows[i]["category"].ToString(),
                            HTML = table.Rows[i]["html"].ToString(),
                            Title = table.Rows[i]["title"].ToString(),
                            Id = (int)table.Rows[i]["id"]
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

    }
}

