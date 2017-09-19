using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using RestSharp;
using ServiceStack;
using ServiceStack.Messaging;
using ServiceStack.Pcl;
using System;
using System.Collections.Generic;
using System.Collections.Specialized;
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
            request.Payload.Token = "xoxp-108334113943-221049390612-242151291554-546c2c932b2d4abfcb662579d3a2b4e0";

            try
            {
                this.MessageProducer3.Publish(new SlackPostMqRequest { Payload = request.Payload , PostType = request.PostType});
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
            public  string Post(SlackPostMqRequest req)
            {
                if (req.PostType == 1)
                {
                    var client = new RestClient("https://slack.com");

                    var request = new RestRequest("api/files.upload", Method.POST);
                    request.AddParameter("token", req.Payload.Token);
                    request.AddParameter("channels", req.Payload.Channel);
                    if (!string.IsNullOrEmpty(req.Payload.Text))
                        request.AddParameter("text", req.Payload.Text);

                    if (req.Payload.SlackFile != null && req.Payload.SlackFile.FileByte != null && req.Payload.SlackFile.FileByte.Length > 0)
                        request.AddFile("file", req.Payload.SlackFile.FileByte, req.Payload.SlackFile.FileName, contentType: "multipart/form-data");

                    //Execute the request
                    var res = client.ExecuteAsyncPost(request, SlackCallBack, "POST");
                }
                
                return null;
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