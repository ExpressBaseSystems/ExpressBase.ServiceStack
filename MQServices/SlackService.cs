using ExpressBase.Common;
using ExpressBase.Objects.Objects.TenantConnectionsRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using RestSharp;
using ServiceStack;
using ServiceStack.Messaging;
using ServiceStack.Pcl;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Net.Http;
using System.Text;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.MQServices
{
    public class SlackService : EbBaseService
    {
        public SlackService(IMessageProducer _mqp) : base(_mqp) { }

        [Authenticate]
        public void Post(SlackPostRequest request)
        {

            try
            {
                this.MessageProducer3.Publish(new SlackPostMqRequest { Payload = request.Payload, PostType = request.PostType, TenantAccountId = request.TenantAccountId, UserId = request.UserId });
                //return true;
            }
            catch (Exception e)
            {
                //return false;
            }
        }

        [Authenticate]
        public void Post(SlackAuthRequest request)
        {
            try
            {

                this.MessageProducer3.Publish(new SlackAuthMqRequest { IsNew = request.IsNew, SlackJson = request.SlackJson, TenantAccountId = request.TenantAccountId, UserId = request.UserId });
                //return true;
            }
            catch (Exception e)
            {
                //return false;
            }
        }

        [Restrict(InternalOnly = true)]
        public class SlackServiceInternal : EbBaseService
        {
            public string Post(SlackAuthMqRequest req)
            {
                if (req.IsNew)
                {
                    TenantDbFactory dbFactory = new TenantDbFactory(req.TenantAccountId, this.Redis);

                    try
                    {
                        string sql = "UPDATE eb_users SET slackjson = @slackjson WHERE id = @id RETURNING id";

                        var id = dbFactory.DataDB.DoQuery<Int32>(sql, new DbParameter[] {
                            dbFactory.DataDB.GetNewParameter("slackjson", NpgsqlTypes.NpgsqlDbType.Json,EbSerializers.Json_Serialize(req.SlackJson)),
                            dbFactory.DataDB.GetNewParameter("id", System.Data.DbType.Int32, req.UserId)
                        });
                    }

                    catch (Exception e)
                    {
                        return null;
                    }
                }
                else
                {

                }
                return null;
            }

            public string Post(SlackPostMqRequest req)
            {

                TenantDbFactory dbFactory = new TenantDbFactory(req.TenantAccountId, this.Redis);

                string sql = "SELECT slackjson FROM eb_users WHERE id = @id";


                var dt = dbFactory.DataDB.DoQuery(sql, new DbParameter[] { dbFactory.DataDB.GetNewParameter("id", System.Data.DbType.Int32, req.UserId) });
                var json = dt.Rows[0][0];
                SlackJson slackJson = JsonConvert.DeserializeObject<SlackJson>(json.ToString());

                var client = new RestClient("https://slack.com");

                var request = new RestRequest("api/files.upload", Method.POST);

                request.AddParameter("token", slackJson.AccessToken);
                request.AddParameter("user_id", slackJson.UserId);
                request.AddParameter("team_id", slackJson.TeamId);
                request.AddParameter("channels", req.Payload.Channel);
                if (!string.IsNullOrEmpty(req.Payload.Text))
                    request.AddParameter("text", req.Payload.Text);

                if (req.Payload.SlackFile != null && req.Payload.SlackFile.FileByte != null && req.Payload.SlackFile.FileByte.Length > 0)
                    request.AddFile("file", req.Payload.SlackFile.FileByte, req.Payload.SlackFile.FileName, contentType: "multipart/form-data");

                //Execute the request
                var res = client.ExecuteAsyncPost(request, SlackCallBack, "POST");



                return null;
            }

            private void AuthRes(IRestResponse arg1, RestRequestAsyncHandle arg2)
            {

            }

            private void SlackCallBack(IRestResponse arg1, RestRequestAsyncHandle arg2)
            {
                //log response...
                //throw new NotImplementedException();
            }


        }


    }
}



//To a take Screenshot of a div (Javascript)
//https://stackoverflow.com/questions/6887183/how-to-take-screenshot-of-a-div-with-javascript