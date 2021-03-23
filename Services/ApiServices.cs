using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.ServiceStack.MQServices;
using Newtonsoft.Json.Linq;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using ExpressBase.Security;
using ServiceStack;
using RestSharp;
using ExpressBase.Objects.Objects;
using System.Net;

namespace ExpressBase.ServiceStack.Services
{
    public class ApiServices : EbBaseService
    {
        private EbObjectService StudioServices { set; get; }

        private Dictionary<string, object> GlobalParams { set; get; }

        private EbApi Api { set; get; }

        private ApiResponse ApiResponse { set; get; }

        private User UserObject { set; get; }

        private string SolutionId { set; get; }

        public ApiServices(IEbConnectionFactory _dbf) : base(_dbf)
        {
            this.StudioServices = base.ResolveService<EbObjectService>();
            this.ApiResponse = new ApiResponse();
        }

        public Dictionary<string, object> ProcessGlobalDictionary(Dictionary<string, object> data)
        {
            Dictionary<string, object> globalParams = new Dictionary<string, object>();

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
            return globalParams;
        }

        [Authenticate]
        public ApiResponse Any(ApiRequest request)
        {
            try
            {
                this.SolutionId = request.SolnId;
                this.GlobalParams = ProcessGlobalDictionary(request.Data);
                this.UserObject = GetUserObject(request.UserAuthId);

                this.GlobalParams["eb_currentuser_id"] = request.UserId;

                if (!this.GlobalParams.ContainsKey("eb_loc_id"))
                {
                    this.GlobalParams["eb_loc_id"] = this.UserObject.Preference.DefaultLocation;
                }

                if (request.HasRefId())
                {
                    try
                    {
                        this.Api = this.GetEbObject<EbApi>(request.RefId);
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"Failed resolve api object from refid '{request.RefId}'");
                        Console.WriteLine(ex.Message);
                    }
                }
                else
                {
                    this.Api = this.Get(new ApiByNameRequest { Name = request.Name, Version = request.Version }).Api;
                }

                if (this.Api == null)
                {
                    this.ApiResponse.Message.ErrorCode = ApiErrorCode.ApiNotFound;
                    this.ApiResponse.Message.Status = "Api does not exist";
                    this.ApiResponse.Message.Description = $"Api '{request.Name}' does not exist!,";

                    throw new Exception(this.ApiResponse.Message.Description);
                }

                InitializeExecution();
            }
            catch (Exception e)
            {
                Console.WriteLine("---API SERVICE END POINT EX CATCH---");
                Console.WriteLine(e.Message + "\n" + e.StackTrace);
            }
            return this.ApiResponse;
        }

        private void InitializeExecution()
        {
            try
            {
                int step = 0;

                int r_count = this.Api.Resources.Count;

                while (step < r_count)
                {
                    this.Api.Resources[step].Result = this.GetResult(this.Api.Resources[step], step);
                    step++;
                }

                this.ApiResponse.Result = this.Api.Resources[step - 1].GetResult();

                this.ApiResponse.Message.Status = "Success";
                this.ApiResponse.Message.ErrorCode = ApiResponse.Result == null ? ApiErrorCode.SuccessWithNoReturn : ApiErrorCode.Success;
                this.ApiResponse.Message.Description = $"Api execution completed, " + this.ApiResponse.Message.Description;
            }
            catch (Exception ex)
            {
                Console.WriteLine("---EXCEPTION AT API-SERVICE [InitializeExecution]---");
                Console.WriteLine(ex.Message);
            }
        }

        private object GetResult(ApiResources resource, int index)
        {
            ResultWrapper res = new ResultWrapper();

            try
            {
                switch (resource)
                {
                    case EbSqlReader reader:
                        res.Result = ExecuteDataReader(reader);
                        break;
                    case EbSqlWriter writer:
                        res.Result = ExecuteDataWriter(writer);
                        break;
                    case EbSqlFunc func:
                        res.Result = ExecuteSqlFunction(func);
                        break;
                    case EbEmailNode email:
                        res.Result = ExecuteEmail(email);
                        break;
                    case EbProcessor processor:
                        res.Result = ExecuteScript(processor, index);
                        break;
                    case EbConnectApi ebApi:
                        res.Result = ExecuteConnectApi(ebApi);
                        break;
                    case EbThirdPartyApi thirdParty:
                        res.Result = ExecuteThirdPartyApi(thirdParty);
                        break;
                    case EbFormResource form:
                        res.Result = ExecuteFormResource(form);
                        break;
                    default:
                        res.Result = null;
                        break;
                }

                return res.Result;
            }
            catch (Exception ex)
            {
                if (ex is ExplicitExitException)
                {
                    this.ApiResponse.Message.Status = "Success";
                    this.ApiResponse.Message.Description = ex.Message;
                    this.ApiResponse.Message.ErrorCode = ApiErrorCode.ExplicitExit;
                }
                else
                {
                    this.ApiResponse.Message.Status = "Error";
                    this.ApiResponse.Message.Description = $"Failed to execute Resource '{resource.Name}' " + ex.Message;
                }

                throw new ApiException("[GetResult] ," + ex.Message);
            }
        }

        private object ExecuteDataReader(EbSqlReader sqlReader)
        {
            EbDataSet dataSet;
            try
            {
                EbDataReader dataReader = GetEbObject<EbDataReader>(sqlReader.Reference);

                List<DbParameter> dbParameters = new List<DbParameter>();

                List<Param> InputParams = dataReader.GetParams((RedisClient)this.Redis);

                FillParams(InputParams);

                foreach (Param param in InputParams)
                {
                    dbParameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter(param.Name, (EbDbTypes)Convert.ToInt32(param.Type), param.ValueTo));
                }

                dataSet = this.EbConnectionFactory.DataDB.DoQueries(dataReader.Sql, dbParameters.ToArray());
            }
            catch (Exception ex)
            {
                throw new ApiException("[ExecuteDataReader], " + ex.Message);
            }
            return dataSet;
        }

        private object ExecuteDataWriter(EbSqlWriter sqlWriter)
        {
            List<DbParameter> dbParams = new List<DbParameter>();
            try
            {
                EbDataWriter dataWriter = GetEbObject<EbDataWriter>(sqlWriter.Reference);

                List<Param> InputParams = dataWriter.GetParams(null);

                FillParams(InputParams);

                foreach (Param param in InputParams)
                {
                    dbParams.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(param.Name, (EbDbTypes)Convert.ToInt32(param.Type), param.ValueTo));
                }

                int status = this.EbConnectionFactory.ObjectsDB.DoNonQuery(dataWriter.Sql, dbParams.ToArray());

                if (status > 0)
                {
                    this.ApiResponse.Message.Description = status + "row inserted";
                    return true;
                }
                else
                {
                    this.ApiResponse.Message.Description = status + "row inserted";
                    return false;
                }
            }
            catch (Exception ex)
            {
                throw new ApiException("[ExecuteDataWriter], " + ex.Message);
            }
        }

        private object ExecuteSqlFunction(EbSqlFunc sqlFunction)
        {
            SqlFuncTestResponse response;
            try
            {
                EbSqlFunction sqlFunc = this.GetEbObject<EbSqlFunction>(sqlFunction.Reference);

                List<Param> InputParams = sqlFunc.GetParams(null);

                FillParams(InputParams);

                DataSourceService DSService = base.ResolveService<DataSourceService>();

                response = DSService.Post(new SqlFuncTestRequest { FunctionName = sqlFunc.Name, Parameters = InputParams });
            }
            catch (Exception ex)
            {
                throw new ApiException("[ExecuteSqlFunction], " + ex.Message);
            }
            return response;
        }

        private object ExecuteScript(EbProcessor processor, int step)
        {
            ApiGlobals global = new ApiGlobals(this.GlobalParams);

            global.ResourceValueHandler += (index) =>
            {
                object resourceValue = this.Api.Resources[index].Result;

                if (resourceValue != null && resourceValue is string converted)
                {
                    if (converted.StartsWith("{") && converted.EndsWith("}") || converted.StartsWith("[") && converted.EndsWith("]"))
                    {
                        string formated = converted.Replace(@"\", string.Empty);
                        return JObject.Parse(formated);
                    }
                }
                return resourceValue;
            };

            ApiResources lastResource = step == 0 ? null : this.Api.Resources[step - 1];

            if (lastResource != null && lastResource.Result != null && lastResource.Result is EbDataSet dataSet)
            {
                global.Tables = dataSet.Tables;
            }

            ApiScript result;

            try
            {
                result = processor.Execute(global);
            }
            catch (Exception ex)
            {
                throw ex;
            }
            return result;
        }

        private bool ExecuteEmail(EbEmailNode emailNode)
        {
            bool status;

            try
            {
                EbEmailTemplate emailTemplate = this.GetEbObject<EbEmailTemplate>(emailNode.Reference);

                List<Param> InputParams = this.GetEmailParams(emailTemplate);

                FillParams(InputParams);

                EmailTemplateSendService EmailService = base.ResolveService<EmailTemplateSendService>();

                EmailService.Post(new EmailTemplateWithAttachmentMqRequest
                {
                    SolnId = this.SolutionId,
                    Params = InputParams,
                    ObjId = Convert.ToInt32(emailNode.Reference.Split(CharConstants.DASH)[3])
                });

                status = true;

                string msg = $"The mail has been sent successfully to {emailTemplate.To} with subject {emailTemplate.Subject} and cc {emailTemplate.Cc}";

                this.ApiResponse.Message.Description = msg;
            }
            catch (Exception ex)
            {
                throw new ApiException("[ExecuteEmail], " + ex.Message);
            }
            return status;
        }

        private object ExecuteConnectApi(EbConnectApi apiResource)
        {
            ApiResponse resp = null;

            try
            {
                EbApi apiObject = GetEbObject<EbApi>(apiResource.Reference);

                if (apiObject.Name.Equals(this.Api.Name))
                {
                    this.ApiResponse.Message.ErrorCode = ApiErrorCode.ResourceCircularRef;
                    this.ApiResponse.Message.Description = "Calling Api from the same not allowed, terminated due to circular reference";

                    throw new ApiException("[ExecuteConnectApi], Circular refernce");
                }
                else
                {
                    List<Param> InputParam = Get(new ApiReqJsonRequest { Components = apiObject.Resources }).Params;

                    FillParams(InputParam);

                    Dictionary<string, object> d = InputParam.Select(p => new { prop = p.Name, val = p.Value }).ToDictionary(x => x.prop, x => x.val as object);

                    ApiServices apiService = base.ResolveService<ApiServices>();

                    resp = apiService.Any(new ApiRequest
                    {
                        Name = apiResource.RefName,
                        Version = apiResource.Version,
                        Data = d
                    });

                    if (resp.Message.ErrorCode == ApiErrorCode.NotFound)
                    {
                        this.ApiResponse.Message.ErrorCode = ApiErrorCode.ResourceNotFound;
                        this.ApiResponse.Message.Description = resp.Message.Description;

                        throw new ApiException("[ExecuteConnectApi], resource api not found");
                    }
                }
            }
            catch (Exception ex)
            {
                throw new ApiException("[ExecuteConnectApi], " + ex.Message);
            }
            return resp;
        }

        private object ExecuteThirdPartyApi(EbThirdPartyApi thirdPartyResource)
        {
            Uri uri = new Uri(thirdPartyResource.Url);

            object result;

            try
            {
                RestClient client = new RestClient(uri.GetLeftPart(UriPartial.Authority));

                RestRequest request = thirdPartyResource.CreateRequest(uri.PathAndQuery, GlobalParams);

                List<Param> parameters = thirdPartyResource.GetParameters(this.GlobalParams) ?? new List<Param>();

                if (thirdPartyResource.Method == ApiMethods.POST && thirdPartyResource.RequestFormat == ApiRequestFormat.Raw)
                {
                    if (parameters.Count > 0)
                    {
                        request.AddJsonBody(parameters[0].Value);
                    }
                }
                else
                {
                    foreach (Param param in parameters)
                    {
                        request.AddParameter(param.Name, param.ValueTo);
                    }
                }

                IRestResponse resp = client.Execute(request);

                if (resp.IsSuccessful)
                    result = resp.Content;
                else
                    throw new Exception($"Failed to execute api [{thirdPartyResource.Url}], {resp.ErrorMessage}, {resp.Content}");
            }
            catch (Exception ex)
            {
                throw new Exception("[ExecuteThirdPartyApi], " + ex.Message);
            }
            return result;
        }

        private object ExecuteFormResource(EbFormResource formResource)
        {
            try
            {
                int RecordId = 0;
                WebFormServices webFormServices = base.ResolveService<WebFormServices>();
                NTVDict _params = new NTVDict();
                foreach(KeyValuePair<string, object> p in this.GlobalParams)
                {
                    EbDbTypes _type;
                    if (p.Value is int)
                        _type = EbDbTypes.Int32;
                    else //check other types here if required
                        _type = EbDbTypes.String;
                    _params.Add(p.Key, new NTV() { Name = p.Key, Type = _type, Value = p.Value });
                }

                if (!string.IsNullOrWhiteSpace(formResource.DataIdParam) && this.GlobalParams.ContainsKey(formResource.DataIdParam))
                {
                    int.TryParse(Convert.ToString(this.GlobalParams[formResource.DataIdParam]), out RecordId);
                }

                InsertOrUpdateFormDataResp resp = webFormServices.Any(new InsertOrUpdateFormDataRqst
                {
                    RefId = formResource.Reference,
                    PushJson = formResource.PushJson,
                    UserId = this.UserObject.UserId,
                    UserAuthId = this.UserObject.AuthId,
                    RecordId = RecordId,
                    LocId = Convert.ToInt32(this.GlobalParams["eb_loc_id"]),
                    SolnId = this.SolutionId,
                    WhichConsole = "uc",
                    FormGlobals = new FormGlobals { Params = _params },
                    //TransactionConnection = TransactionConnection
                });

                if (resp.Status == (int)HttpStatusCode.OK)
                    return resp.RecordId;
                else
                    throw new Exception(resp.Message);
            }
            catch (Exception ex)
            {
                throw new ApiException("[ExecuteFormResource], " + ex.Message);
            }
        }

        private List<Param> GetEmailParams(EbEmailTemplate enode)
        {
            List<Param> p = new List<Param>();

            if (!string.IsNullOrEmpty(enode.AttachmentReportRefID))
            {
                EbReport o = GetEbObject<EbReport>(enode.AttachmentReportRefID);

                if (!string.IsNullOrEmpty(o.DataSourceRefId))
                {
                    EbDataSourceMain ob = GetEbObject<EbDataSourceMain>(o.DataSourceRefId);

                    p = p.Merge(ob.GetParams(this.Redis as RedisClient)).ToList();
                }
            }
            if (!string.IsNullOrEmpty(enode.DataSourceRefId))
            {
                EbDataSourceMain ob = GetEbObject<EbDataSourceMain>(enode.DataSourceRefId);

                p = p.Merge(ob.GetParams(this.Redis as RedisClient)).ToList();
            }
            return p;
        }

        private void FillParams(List<Param> inputParam)
        {
            try
            {
                foreach (Param p in inputParam)
                {
                    object value = this.GetParameterValue(p.Name);

                    if (IsRequired(p.Name))
                    {
                        if (value == null || string.IsNullOrEmpty(value.ToString()))
                        {
                            this.ApiResponse.Message.ErrorCode = ApiErrorCode.ParameterNotFound;
                            this.ApiResponse.Message.Status = $"Parameter Error";

                            throw new ApiException($"Parameter '{p.Name}' must be set");
                        }
                        else p.Value = value.ToString();
                    }
                    else
                    {
                        if (value == null || string.IsNullOrEmpty(value.ToString()))
                            p.Value = null;
                        else
                            p.Value = value.ToString();
                    }
                }
            }
            catch (Exception ex)
            {
                throw new ApiException("[Params], " + ex.Message);
            }
        }

        private bool IsRequired(string name)
        {
            if (this.Api.Request == null)
                return true;

            Param p = this.Api.Request.GetParam(name);

            return p == null || p.Required;
        }

        private object GetParameterValue(string name)
        {
            if (this.GlobalParams.ContainsKey(name))
            {
                return this.GlobalParams[name];
            }
            return null;
        }

        public ApiMetaResponse Get(ApiMetaRequest request)
        {
            this.EbConnectionFactory = new EbConnectionFactory(request.SolutionId, this.Redis);
            this.StudioServices.EbConnectionFactory = this.EbConnectionFactory;
            ApiByNameResponse resp = this.Get(new ApiByNameRequest
            {
                Name = request.Name,
                Version = request.Version,
                SolnId = request.SolutionId
            });

            List<Param> p = new List<Param>();

            p.AddRange(resp.Api.Request.Custom);
            p.AddRange(resp.Api.Request.Default);

            return new ApiMetaResponse { Params = p, Name = request.Name, Version = request.Version };
        }

        public ApiAllMetaResponse Get(ApiAllMetaRequest request)
        {
            ApiAllMetaResponse resp = new ApiAllMetaResponse();
            this.EbConnectionFactory = new EbConnectionFactory(request.SolutionId, this.Redis);
            string sql = @"SELECT 
	                            EO.obj_name,EOV.version_num
                            FROM
	                            eb_objects_ver EOV
                            INNER JOIN
	                            eb_objects EO ON EOV.eb_objects_id = EO.id
                            INNER JOIN
	                            eb_objects_status EOS ON EOS.eb_obj_ver_id = EOV.id
                            WHERE
	                            EO.obj_type=20 
                            AND
	                            EOS.status = 3";
            var dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql);

            foreach (EbDataRow row in dt.Rows)
                resp.AllMetas.Add(new EbObjectWrapper { Name = row[0].ToString(), VersionNumber = row["version_num"].ToString() });

            return resp;
        }

        [Authenticate]
        public ApiReqJsonResponse Get(ApiReqJsonRequest request)
        {
            List<Param> parameters = new List<Param>();

            foreach (ApiResources resource in request.Components)
            {
                if (resource is EbSqlReader || resource is EbSqlWriter || resource is EbSqlFunc)
                {
                    EbDataSourceMain dataSource = this.GetEbObject<EbDataSourceMain>(resource.Reference);

                    if (dataSource.InputParams == null || dataSource.InputParams.Count <= 0)
                        parameters.Merge(dataSource.GetParams(this.Redis as RedisClient));
                    else
                        parameters.Merge(dataSource.InputParams);
                }
                else if (resource is EbEmailNode)
                {
                    EbEmailTemplate emailTemplate = this.GetEbObject<EbEmailTemplate>(resource.Reference);

                    parameters = parameters.Merge(this.GetEmailParams(emailTemplate));
                }
                else if (resource is EbConnectApi)
                {
                    EbApi ob = this.GetEbObject<EbApi>(resource.Reference);

                    parameters = parameters.Merge(this.Get(new ApiReqJsonRequest { Components = ob.Resources }).Params);
                }
                else if (resource is EbThirdPartyApi thirdParty)
                {
                    if (thirdParty.Parameters != null && thirdParty.Parameters.Count > 0)
                    {
                        foreach (var param in thirdParty.Parameters)
                        {
                            parameters.Add(new Param
                            {
                                Name = param.Name,
                                Type = param.Type.ToString(),
                                Value = param.Value
                            });
                        }
                    }
                }
            }
            return new ApiReqJsonResponse { Params = parameters };
        }

        [Authenticate]
        public ApiResponse Post(ApiComponetRequest request)
        {
            try
            {
                this.GlobalParams = request.Params.Select(p => new { prop = p.Name, val = p.ValueTo })
                    .ToDictionary(x => x.prop, x => x.val as object);

                if (request.Component is EbSqlReader reader)
                    request.Component.Result = this.ExecuteDataReader(reader);
                else if (request.Component is EbSqlWriter writer)
                    request.Component.Result = this.ExecuteDataWriter(writer);
                else if (request.Component is EbSqlFunc func)
                    request.Component.Result = this.ExecuteSqlFunction(func);
                else if (request.Component is EbConnectApi ebApi)
                {
                    request.Component.Result = this.Any(new ApiRequest
                    {
                        Name = ebApi.RefName,
                        Version = ebApi.Version,
                        Data = this.GlobalParams
                    });
                }
                else if (request.Component is EbFormResource form)
                    request.Component.Result = this.ExecuteFormResource(form);
                else
                    request.Component.Result = null;

                this.ApiResponse.Result = request.Component.GetResult();
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
                this.ApiResponse.Result = null;
            }
            return this.ApiResponse;
        }

        [Authenticate]
        public ApiByNameResponse Get(ApiByNameRequest request)
        {
            EbApi api_o = null;
            string sql = EbConnectionFactory.ObjectsDB.EB_API_BY_NAME;

            DbParameter[] parameter =
            {
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("objname",EbDbTypes.String,request.Name),
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("version",EbDbTypes.String,request.Version)
            };
            EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql, parameter);
            if (dt.Rows.Count > 0)
            {
                EbDataRow dr = dt.Rows[0];
                EbObjectWrapper _ebObject = (new EbObjectWrapper
                {
                    Json = dr[0].ToString(),
                    VersionNumber = dr[1].ToString(),
                    EbObjectType = (dr[4] != DBNull.Value) ? Convert.ToInt32(dr[4]) : 0,
                    Status = Enum.GetName(typeof(ObjectLifeCycleStatus), Convert.ToInt32(dr[2])),
                    Tags = dr[3].ToString(),
                    RefId = null,
                });
                api_o = EbSerializers.Json_Deserialize<EbApi>(_ebObject.Json);
            }
            return new ApiByNameResponse { Api = api_o };
        }

        private T GetEbObject<T>(string refid)
        {
            T ebObject = this.Redis.Get<T>(refid);

            if (ebObject == null)
            {
                EbObjectParticularVersionResponse obj = (EbObjectParticularVersionResponse)StudioServices.Get(new EbObjectParticularVersionRequest
                {
                    RefId = refid
                });
                ebObject = EbSerializers.Json_Deserialize<T>(obj.Data[0].Json);
            }

            if (ebObject == null)
            {
                string message = $"{typeof(T).Name} not found";
                this.ApiResponse.Message.Description = message;

                throw new ApiException(message);
            }
            return ebObject;
        }
    }
}
