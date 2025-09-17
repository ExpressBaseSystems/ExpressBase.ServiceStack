using System;
using System.Collections.Generic;
using ExpressBase.Common.Data;
using ExpressBase.Common.Helpers;
using ExpressBase.Common.ServiceClients;
using ExpressBase.CoreBase.Globals;
using ExpressBase.Objects;
using ExpressBase.Objects.Services;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceStack;
using ServiceStack.Messaging;

namespace ExpressBase.ServiceStack.MQServices
{
    [Restrict(InternalOnly = true)]
    public class ApiInternalService : EbMqBaseService
    {
        private EbApi Api { set; get; }

        public int LogMasterId { get; set; }

        public ApiInternalService(IMessageProducer _mqp) : base(_mqp)
        {

        }

        public Dictionary<string, object> ProcessGlobalDictionary(Dictionary<string, object> data)
        {
            Dictionary<string, object> globalParams = new Dictionary<string, object>();
            if (!(data is null))
            {
                foreach (KeyValuePair<string, object> kp in data)
                {
                    if (kp.Value == null) continue;

                    if (kp.Value is string parsed)
                    {
                        parsed = parsed.Trim();

                        if ((parsed.StartsWith("{") && parsed.EndsWith("}")) || (parsed.StartsWith("[") && parsed.EndsWith("]")))
                        {
                            string formated = parsed.Replace(@"\", string.Empty);
                            globalParams.Add(kp.Key, JObject.Parse(formated));
                        }
                        else
                            globalParams.Add(kp.Key, kp.Value);
                    }
                    else
                    {
                        globalParams.Add(kp.Key, kp.Value);
                    }
                }
            }
            return globalParams;
        }

        public ApiResponse Any(ApiMqRequest request)
        {
            try
            {
                this.EbConnectionFactory = new EbConnectionFactory(request?.JobArgs?.SolnId, Redis);

                this.LogMasterId = EbApiHelper.InsertLog(request.Name, request.Version, request.RefId ?? request.JobArgs?.ObjId.ToString() ?? request.JobArgs?.RefId, this.EbConnectionFactory.DataDB, 2);

                GetApiObject(request);

                InitializeExecution();
            }
            catch (Exception e)
            {
                Console.WriteLine("---API SERVICE END POINT EX CATCH---");
                Console.WriteLine(e.Message + "\n" + e.StackTrace);
            }

            string paramsUsed = (Api.GlobalParams != null) ? JsonConvert.SerializeObject(Api.GlobalParams) : JsonConvert.SerializeObject(request?.JobArgs?.Params);
            int uId = request.JobArgs.UserId > 0 ? request.JobArgs.UserId : request.UserId;

            EbApiHelper.UpdateLog(this.EbConnectionFactory.DataDB, this.LogMasterId, this.Api.ApiResponse.Message.Description,
                this.Api.ApiResponse.Message.Status, paramsUsed,
                JsonConvert.SerializeObject(this.Api.ApiResponse?.Result), uId);

            return this.Api.ApiResponse;
        }

        public void GetApiObject(ApiMqRequest request)
        {
            int UserId;
            string SolutionId;
            string UserAuthId;
            Dictionary<string, object> ApiData;

            if ((request.HasRefId() || request.HasObjectId()) && request.JobArgs != null)
            {
                SolutionId = request.JobArgs.SolnId;
                UserAuthId = request.JobArgs.UserAuthId;
                ApiData = request.JobArgs.ApiData;
                UserId = request.JobArgs.UserId;

                try
                {
                    this.Api = new EbApi();
                    this.EbConnectionFactory = new EbConnectionFactory(SolutionId, Redis);

                    this.Api = Api.GetApi(request.JobArgs.RefId, request.JobArgs.ObjId, this.Redis, this.EbConnectionFactory.DataDB, this.EbConnectionFactory.ObjectsDB);

                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed resolve api object from refid '{request.JobArgs?.RefId}'");
                    Console.WriteLine(ex.Message);
                }
            }
            else
            {
                SolutionId = request.SolnId;
                UserAuthId = request.UserAuthId;
                ApiData = request.Data;
                UserId = request.UserId;

                try
                {
                    this.EbConnectionFactory = new EbConnectionFactory(SolutionId, Redis);
                    this.Api = EbApiHelper.GetApiByName(request.Name, request.Version, this.EbConnectionFactory.ObjectsDB);
                    if (!(this.Api is null))
                    {
                        Api.Redis = this.Redis;
                        Api.ObjectsDB = this.EbConnectionFactory.ObjectsDB;
                        Api.DataDB = this.EbConnectionFactory.DataDB;
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Failed resolve api object from refid '{this.Api?.RefId}'");
                    Console.WriteLine(ex.Message);
                }
            }

            if (this.Api is null)
            {
                this.Api = new EbApi { };
                this.Api.ApiResponse.Message.ErrorCode = ApiErrorCode.ApiNotFound;
                this.Api.ApiResponse.Message.Status = "Api does not exist";
                this.Api.ApiResponse.Message.Description = $"Api does not exist!,";

                throw new Exception(this.Api.ApiResponse.Message.Description);
            }

            this.Api.SolutionId = SolutionId;
            this.Api.UserObject = GetUserObject(UserAuthId);

            this.Api.GlobalParams = ProcessGlobalDictionary(ApiData);
            this.Api.GlobalParams["eb_currentuser_id"] = UserId;

            if (!this.Api.GlobalParams.ContainsKey("eb_loc_id"))
            {
                this.Api.GlobalParams["eb_loc_id"] = this.Api.UserObject.Preference.DefaultLocation;
            }
        }

        private void InitializeExecution()
        {
            try
            {
                int r_count = this.Api.Resources.Count;

                while (Api.Step < r_count)
                {
                    this.Api.Resources[Api.Step].Result = this.GetResult(this.Api.Resources[Api.Step]);
                    Api.Step++;
                }

                if (this.Api.ApiResponse.Result == null)
                    this.Api.ApiResponse.Result = this.Api.Resources[Api.Step - 1].GetResult();

                this.Api.ApiResponse.Message.Status = "Success";
                this.Api.ApiResponse.Message.ErrorCode = this.Api.ApiResponse.Result == null ? ApiErrorCode.SuccessWithNoReturn : ApiErrorCode.Success;
                this.Api.ApiResponse.Message.Description = $"Api execution completed, " + this.Api.ApiResponse.Message.Description;
            }
            catch (Exception ex)
            {
                Console.WriteLine("---EXCEPTION AT API-SERVICE [InitializeExecution]---");
                Console.WriteLine(ex.Message);
            }
        }

        private object GetResult(ApiResources resource)
        {
            try
            {
                return EbApiHelper.GetResult(resource, this.Api, MessageProducer3, this, this.FileClient);
            }
            catch (Exception ex)
            {
                if (ex is ExplicitExitException)
                {
                    this.Api.ApiResponse.Message.Status = "Success";
                    this.Api.ApiResponse.Message.Description = ex.Message;
                    this.Api.ApiResponse.Message.ErrorCode = ApiErrorCode.ExplicitExit;
                }
                else
                {
                    this.Api.ApiResponse.Message.Status = "Error";
                    this.Api.ApiResponse.Message.Description = $"Failed to execute Resource '{resource.Name}' " + ex.Message;
                }

                throw new ApiException("[GetResult] ," + ex.Message);
            }
        }
    }
}
