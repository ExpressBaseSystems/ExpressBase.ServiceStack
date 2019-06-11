using ExpressBase.Common;
using ExpressBase.Common.Connections;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
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
            eb_wiki2 (
                    category, title, html, created_by, eb_tags) 
            VALUES (
                    @category, @title, @html, @createdby , @tags)
            RETURNING id";

                DbParameter[] parameters = new DbParameter[]
                {
                this.InfraConnectionFactory.DataDB.GetNewParameter("category", EbDbTypes.String, request.Wiki.Category),
                this.InfraConnectionFactory.DataDB.GetNewParameter("title", EbDbTypes.String, request.Wiki.Title),
                this.InfraConnectionFactory.DataDB.GetNewParameter("html", EbDbTypes.String, request.Wiki.HTML),
                this.InfraConnectionFactory.DataDB.GetNewParameter("createdby", EbDbTypes.Int32, request.Wiki.CreatedBy),
                this.InfraConnectionFactory.DataDB.GetNewParameter("tags", EbDbTypes.String, request.Wiki.Tags)
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
            UPDATE eb_wiki2 SET
                category= @category, title = @title , html = @html , eb_updated_by = @createdby, eb_updated_on = @updatedtime, eb_tags = @tags
            WHERE 
                id= @id
            RETURNING id";

                DbParameter[] parameters = new DbParameter[]
                {
                this.InfraConnectionFactory.DataDB.GetNewParameter("category", EbDbTypes.String, request.Wiki.Category),
                this.InfraConnectionFactory.DataDB.GetNewParameter("title", EbDbTypes.String, request.Wiki.Title),
                this.InfraConnectionFactory.DataDB.GetNewParameter("html", EbDbTypes.String, request.Wiki.HTML),
                this.InfraConnectionFactory.DataDB.GetNewParameter("createdby", EbDbTypes.Int32, request.Wiki.CreatedBy),
                this.InfraConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Wiki.Id),
                 this.InfraConnectionFactory.DataDB.GetNewParameter("tags", EbDbTypes.String, request.Wiki.Tags),
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
                    eb_wiki2  
                 WHERE
                   id = @id AND eb_del='false' ";

                EbDataTable table = InfraConnectionFactory.DataDB.DoQuery(query, parameters);

                resp.Wiki.Category = table.Rows[0]["category"].ToString();
                resp.Wiki.Title = table.Rows[0]["title"].ToString();
                resp.Wiki.HTML = table.Rows[0]["html"].ToString();
                resp.Wiki.Tags = table.Rows[0]["eb_tags"].ToString();


            }
            catch (Exception e)
            {

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
                    eb_wiki2
                WHERE
                    title LIKE '%' || @search_wiki || '%'  ";

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
                    eb_wiki2 
                WHERE 
                    eb_del='false'";
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
                    eb_wiki2  
                 WHERE
                    id = @id AND eb_del='false' ";

                EbDataTable table = InfraConnectionFactory.DataDB.DoQuery(query, parameters);

                resp.Wiki.Category = table.Rows[0]["category"].ToString();
                resp.Wiki.Title = table.Rows[0]["title"].ToString();
                resp.Wiki.HTML = table.Rows[0]["html"].ToString();

            }
            catch (Exception e)
            {

            }
            return resp;
        }

    }
}

