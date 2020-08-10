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

namespace ExpressBase.ServiceStack.Services
{
    public class ApiServices : EbBaseService
    {
        private EbObjectService StudioServices { set; get; }

        private Dictionary<string, object> GlobalParams { set; get; }

        private Dictionary<string, object> TempParams { set; get; }

        private EbApi Api { set; get; }

        private ApiResponse ApiResponse { set; get; }

        private User UserObject { set; get; }

        private string SolutionId { set; get; }

        public ApiServices(IEbConnectionFactory _dbf) : base(_dbf)
        {
            this.StudioServices = base.ResolveService<EbObjectService>();
            this.ApiResponse = new ApiResponse();
        }

        //json object formating
        public Dictionary<string, object> Proc(Dictionary<string, object> _d)
        {
            Dictionary<string, object> gp = new Dictionary<string, object>();
            foreach (var kp in _d)
            {
                if ((kp.Value as string).StartsWith("{") && (kp.Value as string).EndsWith("}") ||
                    (kp.Value as string).StartsWith("[") && (kp.Value as string).EndsWith("]"))
                {
                    string formated = (kp.Value as string).Replace(@"\", string.Empty);
                    gp.Add(kp.Key, JObject.Parse(formated));
                }
                else
                    gp.Add(kp.Key, kp.Value);
            }
            return gp;
        }

        //api execution init
        [Authenticate]
        public ApiResponse Any(ApiRequest request)
        {
            try
            {
                this.SolutionId = request.SolnId;
                this.GlobalParams = this.Proc(request.Data);
                this.UserObject = GetUserObject(request.UserAuthId);

                //fill default param
                this.GlobalParams["eb_currentuser_id"] = request.UserId;

                if (!this.GlobalParams.ContainsKey("eb_loc_id"))
                {
                    this.GlobalParams["eb_loc_id"] = this.UserObject.Preference.DefaultLocation;
                }

                int step = 0;
                this.Api = this.Get(new ApiByNameRequest { Name = request.Name, Version = request.Version }).Api;

                if (Api != null)
                {
                    int r_count = this.Api.Resources.Count;
                    while (step < r_count)
                    {
                        this.Api.Resources[step].Result = this.GetResult(this.Api.Resources[step], step);
                        step++;
                    }
                    this.ApiResponse.Result = this.Api.Resources[step - 1].GetResult();
                    this.ApiResponse.Message.Status = ApiConstants.SUCCESS;
                    if (this.ApiResponse.Result != null)
                        this.ApiResponse.Message.ErrorCode = ApiErrorCode.Success;
                }
                else
                {
                    this.ApiResponse.Message.Status = "Unknown Api";
                    this.ApiResponse.Message.Description = string.Format(ApiConstants.API_NOTFOUND, request.Name);
                    this.ApiResponse.Message.ErrorCode = ApiErrorCode.NotFound;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message);
            }
            return this.ApiResponse;
        }

        private object GetResult(ApiResources resource, int index)
        {
            ResultWrapper res = new ResultWrapper();
            try
            {
                if (resource is EbSqlReader)
                    res.Result = this.ExcDataReader(resource as EbSqlReader, index);
                else if (resource is EbSqlWriter)
                    res.Result = this.ExcDataWriter(resource as EbSqlWriter, index);
                else if (resource is EbSqlFunc)
                    res.Result = this.ExcSqlFunction(resource as EbSqlFunc, index);
                else if (resource is EbEmailNode)
                    res.Result = this.ExcEmail(resource as EbEmailNode, index);
                else if (resource is EbProcessor)
                    res.Result = (resource as EbProcessor).Evaluate((index != 0) ? this.Api.Resources[index - 1] : null, this.GlobalParams);
                else if (resource is EbConnectApi)
                    res.Result = this.ExecuteConnectApi((resource as EbConnectApi), index);
                else if (resource is EbThirdPartyApi)
                    res.Result = (resource as EbThirdPartyApi).Execute();
                else if (resource is EbFormResource)
                    res.Result = this.ExecuteFormResource((resource as EbFormResource), index);
                return res.Result;
            }
            catch (Exception e)
            {
                if (e is ExplicitExitException)
                {
                    this.ApiResponse.Message.Status = ApiConstants.SUCCESS;
                    this.ApiResponse.Message.Description = e.Message;
                    this.ApiResponse.Message.ErrorCode = ApiErrorCode.ExplicitExit;
                }
                else
                    this.ApiResponse.Message.ErrorCode = ApiErrorCode.Failed;
                throw new ApiException();
            }
        }

        //execute eb sql datareader object
        private object ExcDataReader(EbSqlReader sqlreader, int step_c)
        {
            EbDataSet dt;
            try
            {
                ObjWrapperInt ObjectWrapper = this.GetObjectByVer(sqlreader.Reference);
                if (ObjectWrapper.EbObj == null)
                {
                    this.ApiResponse.Message.Description = "DataReader not found";
                    throw new ApiException("DataReader not found");
                }
                List<DbParameter> p = new List<DbParameter>();
                List<Param> InputParams = (ObjectWrapper.EbObj as EbDataReader).GetParams(this.Redis as RedisClient);
                this.FillParams(InputParams, step_c);//fill parameter value from prev component

                foreach (Param pr in InputParams)
                    p.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(pr.Name, (EbDbTypes)Convert.ToInt32(pr.Type), pr.ValueTo));

                dt = this.EbConnectionFactory.ObjectsDB.DoQueries((ObjectWrapper.EbObj as EbDataReader).Sql, p.ToArray());
                this.ApiResponse.Message.Description = ApiConstants.EXE_SUCCESS;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                this.ApiResponse.Message.Description = $"{ex.Message}. Error at DataReader, Resource position {step_c + 1}";
                throw new ApiException("Excecution Failed");
            }
            return dt;
        }

        //execute eb datawriter object
        private object ExcDataWriter(EbSqlWriter writer, int step)
        {
            List<DbParameter> p = new List<DbParameter>();
            try
            {
                ObjWrapperInt ObjectWrapper = this.GetObjectByVer(writer.Reference);
                if (ObjectWrapper.EbObj == null)
                {
                    this.ApiResponse.Message.Description = "DataWriter not found";
                    throw new ApiException();
                }
                List<Param> InputParams = (ObjectWrapper.EbObj as EbDataWriter).GetParams(null);

                this.FillParams(InputParams, step);//fill parameter value from prev component

                foreach (Param pr in InputParams)
                    p.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(pr.Name, (EbDbTypes)Convert.ToInt32(pr.Type), pr.ValueTo));

                int status = this.EbConnectionFactory.ObjectsDB.DoNonQuery((ObjectWrapper.EbObj as EbDataWriter).Sql, p.ToArray());

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
                this.ApiResponse.Message.Description = $"{ex.Message}. Error at DataReader, Resource position {step + 1}";
                Console.WriteLine(ex.Message);
                throw new ApiException("Excecution Failed");
            }
        }

        //execute eb sql function object
        private object ExcSqlFunction(EbSqlFunc sqlfunction, int step)
        {
            var DSService = base.ResolveService<DataSourceService>();
            ObjWrapperInt ObjectWrapper;
            List<Param> InputParams;
            try
            {
                ObjectWrapper = this.GetObjectByVer(sqlfunction.Reference);
                if (ObjectWrapper.EbObj == null)
                {
                    this.ApiResponse.Message.Description = "SqlFunction not found";
                    throw new ApiException("SqlFunction not found");
                }
                InputParams = (ObjectWrapper.EbObj as EbSqlFunction).GetParams(null);

                this.FillParams(InputParams, step);//fill parameter value from prev component
            }
            catch (Exception ex)
            {
                this.ApiResponse.Message.Description = $"{ex.Message}. Error at SqlFunction, Resource position {step + 1}";
                throw new ApiException("Execution Failed:");
            }
            return DSService.Post(new SqlFuncTestRequest { FunctionName = (ObjectWrapper.EbObj as EbSqlFunction).Name, Parameters = InputParams });
        }

        //execute email template object
        private bool ExcEmail(EbEmailNode template, int step)
        {
            var EmailService = base.ResolveService<EmailTemplateSendService>();
            bool stat;
            try
            {
                ObjWrapperInt ObjectWrapper = this.GetObjectByVer(template.Reference);
                if (ObjectWrapper.EbObj == null)
                {
                    this.ApiResponse.Message.Description = "EmailTemplate not found";
                    throw new ApiException("EmailTemplate not found");
                }

                List<Param> InputParams = this.GetEmailParams((ObjectWrapper.EbObj as EbEmailTemplate));

                this.FillParams(InputParams, step);//fill parameter value from prev component

                EmailService.Post(new EmailTemplateWithAttachmentMqRequest
                {
                    SolnId = this.SolutionId,
                    Params = InputParams,
                    ObjId = Convert.ToInt32(template.Reference.Split(CharConstants.DASH)[3])
                });
                stat = true;
                this.ApiResponse.Message.Description = string.Format(ApiConstants.MAIL_SUCCESS,
                                                        (ObjectWrapper.EbObj as EbEmailTemplate).To,
                                                        (ObjectWrapper.EbObj as EbEmailTemplate).Subject,
                                                        (ObjectWrapper.EbObj as EbEmailTemplate).Cc);
            }
            catch (Exception ex)
            {
                this.ApiResponse.Message.Description = $"{ex.Message}. Error at Email, Resource position {step + 1}";
                throw new ApiException(ex.Message);
            }
            return stat;
        }

        //execute connect api
        private object ExecuteConnectApi(EbConnectApi c_api, int step)
        {
            ApiResponse resp = null;
            ObjWrapperInt ObjectWrapper = null;
            try
            {
                ObjectWrapper = this.GetObjectByVer(c_api.Reference);
                if ((ObjectWrapper.EbObj as EbApi).Name.Equals(this.Api.Name))
                {
                    this.ApiResponse.Message.Description = string.Format(ApiConstants.CIRCULAR_REF, this.Api.Name) + ", " + string.Format(ApiConstants.DESCRPT_ERR, step, "ConnectApi", this.Api.Name);
                    throw new ApiException();
                }
                else
                {
                    List<Param> InputParam = this.Get(new ApiReqJsonRequest { Components = (ObjectWrapper.EbObj as EbApi).Resources }).Params;
                    this.FillParams(InputParam, step);
                    Dictionary<string, object> d = InputParam.Select(p => new { prop = p.Name, val = p.Value })
                                                    .ToDictionary(x => x.prop, x => x.val as object);
                    ApiServices s = base.ResolveService<ApiServices>();
                    resp = s.Any(new ApiRequest
                    {
                        Name = c_api.RefName,
                        Version = c_api.Version,
                        Data = d
                    });

                    if (resp.Message.ErrorCode == ApiErrorCode.NotFound)
                    {
                        this.ApiResponse.Message.Description = string.Format(ApiConstants.DESCRPT_ERR, c_api.RouteIndex, "Api", c_api.RefName);
                        throw new ApiException(ApiConstants.API_NOTFOUND);
                    }
                    else if (resp.Message.ErrorCode == ApiErrorCode.Success)
                        this.ApiResponse.Message.Description = ApiConstants.EXE_SUCCESS;
                }
            }
            catch (Exception ex)
            {
                this.ApiResponse.Message.Description = $"{ex.Message}. Error at EbApi, Resource position {step + 1}";
                throw new ApiException(ex.Message);
            }
            return resp;
        }

        private object ExecuteFormResource(EbFormResource formResource, int step)
        {
            try
            {

            }
            catch (Exception ex)
            {
                this.ApiResponse.Message.Description = $"{ex.Message}. Error at Form, Resource position {step + 1}";
                throw new ApiException(ex.Message);
            }
            return null;
        }

        //get email parameters
        private List<Param> GetEmailParams(EbEmailTemplate enode)
        {
            List<Param> p = new List<Param>();
            if (!string.IsNullOrEmpty(enode.AttachmentReportRefID))
            {
                EbReport o = this.GetObjectByVer(enode.AttachmentReportRefID).EbObj as EbReport;
                if (!string.IsNullOrEmpty(o.DataSourceRefId))
                {
                    ObjWrapperInt ob = this.GetObjectByVer(o.DataSourceRefId);
                    p = p.Merge((ob.EbObj as EbDataSourceMain).GetParams(this.Redis as RedisClient)).ToList();
                }
            }
            if (!string.IsNullOrEmpty(enode.DataSourceRefId))
            {
                ObjWrapperInt ob = this.GetObjectByVer(enode.DataSourceRefId);
                p = p.Merge((ob.EbObj as EbDataSourceMain).GetParams(this.Redis as RedisClient)).ToList();
            }
            return p;
        }

        //fill inputparam
        private void FillParams(List<Param> InputParam, int step)
        {
            try
            {
                if (step != 0 && this.Api.Resources[step - 1].Result != null)
                {
                    List<Param> OutParams = this.Api.Resources[step - 1].GetOutParams(InputParam);

                    this.TempParams = OutParams.Select(i => new { prop = i.Name, val = i.ValueTo })
                            .ToDictionary(x => x.prop, x => x.val as object);
                }

                foreach (Param p in InputParam)
                {
                    object value = this.GetParameterValue(p.Name);

                    if (IsRequired(p.Name))
                    {
                        if (value == null || string.IsNullOrEmpty(value.ToString()))
                        {
                            this.ApiResponse.Message.Status = $"Parameter Error";
                            throw new ApiException($"parameter '{p.Name}' is required type and it must be set");
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
                this.ApiResponse.Message.Description = ex.Message;
                throw new ApiException(ex.Message);
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
            try
            {
                if (this.TempParams != null && this.TempParams.ContainsKey(name))
                {
                    return this.TempParams[name];
                }
                else if (this.GlobalParams.ContainsKey(name))
                {
                    return this.GlobalParams[name];
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("API GetParameterValue method exception");
                Console.WriteLine(ex.Message + "\n" + ex.StackTrace);
            }
            return null;
        }

        //api single meta info
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

        //api all meta info
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

        //request json service
        [Authenticate]
        public ApiReqJsonResponse Get(ApiReqJsonRequest request)
        {
            List<Param> p = new List<Param>();
            foreach (ApiResources r in request.Components)
            {
                if (r is EbSqlReader || r is EbSqlWriter || r is EbSqlFunc)
                {
                    ObjWrapperInt obj = this.GetObjectByVer(r.Reference);
                    if ((obj.EbObj as EbDataSourceMain).InputParams == null || (obj.EbObj as EbDataSourceMain).InputParams.Count <= 0)
                        p.Merge((obj.EbObj as EbDataSourceMain).GetParams(this.Redis as RedisClient));
                    else
                        p.Merge((obj.EbObj as EbDataSourceMain).InputParams);
                }
                else if (r is EbEmailNode)
                {
                    ObjWrapperInt obj = this.GetObjectByVer(r.Reference);
                    EbEmailTemplate enode = (EbEmailTemplate)obj.EbObj;
                    p = p.Merge(this.GetEmailParams(enode));
                }
                else if (r is EbConnectApi)
                {
                    ObjWrapperInt ob = this.GetObjectByVer(r.Reference);
                    p = p.Merge(this.Get(new ApiReqJsonRequest { Components = (ob.EbObj as EbApi).Resources }).Params);
                }
            }
            return new ApiReqJsonResponse { Params = p };
        }

        //api component execute
        [Authenticate]
        public ApiResponse Post(ApiComponetRequest request)
        {
            try
            {
                this.GlobalParams = request.Params.Select(p => new { prop = p.Name, val = p.ValueTo })
                    .ToDictionary(x => x.prop, x => x.val as object);
                ObjWrapperInt ow = this.GetObjectByVer(request.Component.Reference);
                if (request.Component is EbSqlReader)
                    request.Component.Result = this.ExcDataReader(request.Component as EbSqlReader, 0);
                else if (request.Component is EbSqlWriter)
                    request.Component.Result = this.ExcDataWriter(request.Component as EbSqlWriter, 0);
                else if (request.Component is EbSqlFunc)
                    request.Component.Result = this.ExcSqlFunction(request.Component as EbSqlFunc, 0);
                else if (request.Component is EbConnectApi)
                {
                    request.Component.Result = this.Any(new ApiRequest
                    {
                        Name = (request.Component as EbConnectApi).RefName,
                        Version = (request.Component as EbConnectApi).Version,
                        Data = this.GlobalParams
                    });
                }
                else if (request.Component is EbFormResource)
                    request.Component.Result = this.ExecuteFormResource(request.Component as EbFormResource, 0);
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

        //find api by name and version
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

        //common object by ref serevice for api
        private ObjWrapperInt GetObjectByVer(string refid)
        {
            EbObjectParticularVersionResponse resp = (EbObjectParticularVersionResponse)StudioServices.Get(new EbObjectParticularVersionRequest { RefId = refid });
            return new ObjWrapperInt
            {
                ObjectType = resp.Data[0].EbObjectType,
                EbObj = EbSerializers.Json_Deserialize(resp.Data[0].Json)
            };
        }
    }
}
