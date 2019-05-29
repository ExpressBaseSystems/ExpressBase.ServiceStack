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
        feature,
        contenttitle,
        content) 
VALUES (
        @feature, @contenttitle, @content)
RETURNING 
    id";

                DbParameter[] parameters = new DbParameter[]
                {
                this.InfraConnectionFactory.DataDB.GetNewParameter("feature", EbDbTypes.String, request.Wiki.Category),
                this.InfraConnectionFactory.DataDB.GetNewParameter("contenttitle", EbDbTypes.String, request.Wiki.Title),
                this.InfraConnectionFactory.DataDB.GetNewParameter("content", EbDbTypes.String, request.Wiki.HTML)
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

            resp.Wiki = request.Wiki;
            try
            {
                DbParameter[] parameters = new DbParameter[]
                    {
                this.InfraConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.Wiki.Id)
                    };
                string query = @"
                 SELECT *
                 FROM
                 eb_wiki2  
                 WHERE
                   id = @id";

                EbDataTable table = InfraConnectionFactory.DataDB.DoQuery(query, parameters);

                resp.Wiki.Category = table.Rows[0]["feature"].ToString();
                resp.Wiki.Title = table.Rows[0]["contenttitle"].ToString();
                resp.Wiki.HTML = table.Rows[0]["content"].ToString();

            }
            catch (Exception e)
            {

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
                eb_wiki2";
                EbDataTable table = InfraConnectionFactory.DataDB.DoQuery(query);

                int capacity = table.Rows.Capacity;

                for (int i = 0; i < capacity; i++)
                {
                    resp.WikiList.Add(
                        new Wiki()
                        {
                            Category = table.Rows[i]["feature"].ToString(),
                            HTML = table.Rows[i]["content"].ToString(),
                            Title = table.Rows[i]["contenttitle"].ToString(),
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





        //public GetTitleResponse Post(GetTitleRequest request)
        //{
        //    GetTitleResponse gtr = new GetTitleResponse();
        //    using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
        //    {
        //        int i = 0;
        //        con.Open();
        //        string query = "select feature from eb_wiki2";
        //        // DbCommand cmd1 = new EbConnectionFactory(EbConnectionsConfigProvider.InfraConnections, "").DataDB.GetNewCommand(con, query);
        //        EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(query);
        //        foreach (EbDataRow row in dt.Rows)
        //        {
        //            gtr.Title.Add(row[0].ToString());
        //        }

        //    }
        //    return gtr;
        //}

        //public GetContentResponse Post(GetContentRequest request)
        //{
        //    GetContentResponse gcr = new GetContentResponse();
        //    using (DbConnection con = this.InfraConnectionFactory.DataDB.GetNewConnection())
        //    {
        //        int i = 0;
        //        con.Open();
        //        string query = "select content from eb_wiki2";
        //        // DbCommand cmd1 = new EbConnectionFactory(EbConnectionsConfigProvider.InfraConnections, "").DataDB.GetNewCommand(con, query);
        //        EbDataTable dt = InfraConnectionFactory.DataDB.DoQuery(query);
        //        foreach (EbDataRow row in dt.Rows)
        //        {
        //            gcr.Content.Add(row[0].ToString());
        //        }

        //    }
        //    return gcr;
        //}

    }

}

