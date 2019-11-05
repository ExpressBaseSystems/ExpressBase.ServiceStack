using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.EmailRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.ServiceStack.MQServices;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using ExpressBase.Security;

namespace ExpressBase.ServiceStack.Services
{
    public class ApiServices : EbBaseService
    {
        private EbObjectService StudioServices { set; get; }

        private Dictionary<string, object> GlobalParams { set; get; }

        private Dictionary<string, object> TempParams { set; get; }

        private EbApi Api { set; get; }

        private ApiResponse ApiResponse { set; get; }

        private string SolutionId { set; get; }

        public ApiServices(IEbConnectionFactory _dbf) : base(_dbf)
        {
            this.StudioServices = base.ResolveService<EbObjectService>();
            this.ApiResponse = new ApiResponse();
        }

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

        public ApiResponse Any(ApiRequest request)
        {
            this.SolutionId = request.SolnId;
            this.GlobalParams = this.Proc(request.Data);
            int step = 0;
            this.Api = this.Get(new ApiByNameRequest { Name = request.Name, Version = request.Version }).Api;
            try
            {
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
                this.ApiResponse.Message.ErrorCode = ApiErrorCode.Failed;
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
                return res.Result;
            }
            catch (Exception e)
            {
                throw new ApiException();
            }
        }

        private object ExcDataReader(EbSqlReader sqlreader, int step_c)
        {
            ObjWrapperInt ObjectWrapper = null;
            EbDataSet dt = null;
            try
            {
                ObjectWrapper = this.GetObjectByVer(sqlreader.Reference);
                if (ObjectWrapper.EbObj == null)
                {
                    this.ApiResponse.Message.Description = "DataReader not found";
                    throw new ApiException("DataReader not found");
                }
                List<DbParameter> p = new List<DbParameter>();
                List<Param> InputParams = (ObjectWrapper.EbObj as EbDataReader).GetParams(this.Redis as RedisClient);
                this.FillParams(InputParams, step_c);//fill parameter value from prev component
                foreach (Param pr in InputParams)
                {
                    p.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(pr.Name, (EbDbTypes)Convert.ToInt32(pr.Type), pr.ValueTo));
                }
                dt = this.EbConnectionFactory.ObjectsDB.DoQueries((ObjectWrapper.EbObj as EbDataReader).Sql, p.ToArray());
                this.ApiResponse.Message.Description = ApiConstants.EXE_SUCCESS;
            }
            catch (Exception e)
            {
                this.ApiResponse.Message.Description = string.Format(ApiConstants.DESCRPT_ERR, step_c, "DataReader", ObjectWrapper.EbObj.Name);
                throw new ApiException("Excecution Failed");
            }
            return dt;
        }

        private object ExcDataWriter(EbSqlWriter writer, int step)
        {
            ObjWrapperInt ObjectWrapper = null;
            List<DbParameter> p = new List<DbParameter>();
            try
            {
                ObjectWrapper = this.GetObjectByVer(writer.Reference);
                if (ObjectWrapper.EbObj == null)
                {
                    this.ApiResponse.Message.Description = "DataWriter not found";
                    throw new ApiException();
                }
                List<Param> InputParams = (ObjectWrapper.EbObj as EbDataWriter).GetParams(null);

                this.FillParams(InputParams, step);//fill parameter value from prev component

                foreach (Param pr in InputParams)
                {
                    p.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(pr.Name, (EbDbTypes)Convert.ToInt32(pr.Type), pr.ValueTo));
                }

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
            catch (Exception e)
            {
                this.ApiResponse.Message.Description = string.Format(ApiConstants.DESCRPT_ERR, step, " DataWriter", ObjectWrapper.EbObj.Name);
                Console.WriteLine(e.Message);
                throw new ApiException("Excecution Failed");
            }
        }

        private object ExcSqlFunction(EbSqlFunc sqlfunction, int step)
        {
            var DSService = base.ResolveService<DataSourceService>();
            ObjWrapperInt ObjectWrapper = null;
            List<Param> InputParams = null;
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
            catch
            {
                this.ApiResponse.Message.Description = string.Format(ApiConstants.DESCRPT_ERR, step, " SqlFunction", ObjectWrapper.EbObj.Name);
                throw new ApiException("Execution Failed:");
            }
            return DSService.Post(new SqlFuncTestRequest { FunctionName = (ObjectWrapper.EbObj as EbSqlFunction).Name, Parameters = InputParams });
        }

        private bool ExcEmail(EbEmailNode template, int step)
        {
            var EmailService = base.ResolveService<PdfToEmailService>();
            ObjWrapperInt ObjectWrapper = null;
            bool stat = false;
            try
            {
                ObjectWrapper = this.GetObjectByVer(template.Reference);
                if (ObjectWrapper.EbObj == null)
                {
                    this.ApiResponse.Message.Description = "EmailTemplate not found";
                    throw new ApiException("EmailTemplate not found");
                }

                List<Param> InputParams = this.GetEmailParams((ObjectWrapper.EbObj as EbEmailTemplate));

                this.FillParams(InputParams, step);//fill parameter value from prev component

                EmailService.Post(new EmailAttachmentMqRequest
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
            catch (Exception e)
            {
                stat = false;
                this.ApiResponse.Message.Description = string.Format(ApiConstants.DESCRPT_ERR, step, "Mail", ObjectWrapper.EbObj.Name);
                throw new ApiException(e.Message);
            }
            return stat;
        }

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
                    {
                        this.ApiResponse.Message.Description = ApiConstants.EXE_SUCCESS;
                    }
                }
            }
            catch (Exception e)
            {
                this.ApiResponse.Message.Description = string.Format(ApiConstants.DESCRPT_ERR, c_api.RouteIndex, "Api", c_api.RefName);
                throw new ApiException(e.Message);
            }
            return resp;
        }

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

        private void FillParams(List<Param> InputParam, int step)
        {
            if (step != 0 && this.Api.Resources[step - 1].Result != null)
            {
                List<Param> OutParams = this.Api.Resources[step - 1].GetOutParams(InputParam);
                this.TempParams = OutParams.Select(i => new { prop = i.Name, val = i.ValueTo })
                        .ToDictionary(x => x.prop, x => x.val as object);
            }

            foreach (Param p in InputParam)
            {
                if (this.TempParams != null && this.TempParams.ContainsKey(p.Name))
                    p.Value = this.TempParams[p.Name].ToString();
                else if (this.GlobalParams.ContainsKey(p.Name))
                    p.Value = this.GlobalParams[p.Name].ToString();
                else if (string.IsNullOrEmpty(p.Value))
                {
                    this.ApiResponse.Message.Status = string.Format(ApiConstants.UNSET_PARAM, p.Name);
                    throw new ApiException(((int)ApiErrorCode.Failed).ToString());
                }
                else
                {
                    this.ApiResponse.Message.Status = string.Format(ApiConstants.UNSET_PARAM, p.Name);
                    throw new ApiException(((int)ApiErrorCode.Failed).ToString());
                }

            }
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
            resp.Api.Request.Custom.ForEach(n => p.Add(n));
            resp.Api.Request.Default.ForEach(n => p.Add(n));

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
            {
                resp.AllMetas.Add(new EbObjectWrapper { Name = row[0].ToString(), VersionNumber = row["version_num"].ToString() });
            }

            return resp;
        }

        //request json service
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
                this.ApiResponse.Result = request.Component.GetResult();
            }
            catch (Exception e)
            {
                this.ApiResponse.Message.Status = "Error";
                this.ApiResponse.Message.Description = e.Message;
                this.ApiResponse.Result = null;
            }
            return this.ApiResponse;
        }

        //find api by name and version
        public ApiByNameResponse Get(ApiByNameRequest request)
        {
            EbApi api_o = null;
            string sql = @"SELECT 
	                            EOV.obj_json,EOV.version_num,EOS.status,EO.obj_tags, EO.obj_type
                            FROM
	                            eb_objects_ver EOV
                            INNER JOIN
	                            eb_objects EO ON EOV.eb_objects_id = EO.id
                            INNER JOIN
	                            eb_objects_status EOS ON EOS.eb_obj_ver_id = EOV.id
                            WHERE
	                            EO.obj_type=20 
                            AND
	                            EO.obj_name=:objname
                            AND 
	                            EOV.version_num =:version
                            LIMIT 1;";

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

        public FormDataJsonResponse Post(FormDataJsonRequest request)
        {
            int _name_c = 1;
            UniqueObjectNameCheckResponse uniqnameresp;
            EbObjectService _studio_serv = base.ResolveService<EbObjectService>();
            WebFormSchema schema = JsonConvert.DeserializeObject<WebFormSchema>(request.JsonData);
            EbSqlFunction obj = new EbSqlFunction(schema, this.EbConnectionFactory);
            string _json = EbSerializers.Json_Serialize(obj);

            do
            {
                uniqnameresp = _studio_serv.Get(new UniqueObjectNameCheckRequest { ObjName = obj.Name });
                if (!uniqnameresp.IsUnique)
                {
                    obj.Name = obj.Name.Remove(obj.Name.Length - 1) + _name_c++;
                    obj.DisplayName = obj.Name;
                }
            }
            while (uniqnameresp.IsUnique);

            EbObject_Create_New_ObjectRequest ds = new EbObject_Create_New_ObjectRequest
            {
                Name = obj.Name,
                Description = obj.Description,
                Json = _json,
                Status = ObjectLifeCycleStatus.Live,
                Relations = null,
                IsSave = true,
                Tags = null,
                Apps = string.Empty,
                SourceSolutionId = request.SolnId,
                SourceObjId = "0",
                SourceVerID = "0",
                DisplayName = obj.DisplayName,
                SolnId = request.SolnId,
                UserId = request.UserId
            };

            EbObject_Create_New_ObjectResponse res = _studio_serv.Post(ds);

            return new FormDataJsonResponse { RefId = res.RefId };
        }

        //generate insert obj and update object
        private void GenJsonColumns(WebformData data)
        {
            FormSqlData sqlData = new FormSqlData();

            foreach (KeyValuePair<string, SingleTable> kp in data.MultipleTables)
            {
                List<JsonColVal> insertcols = new List<JsonColVal>();
                List<JsonColVal> updatecols = new List<JsonColVal>();

                foreach (SingleRow _row in kp.Value)
                {
                    JsonColVal jsoncols_ins = new JsonColVal();
                    JsonColVal jsoncols_upd = new JsonColVal();

                    if (_row.IsUpdate)
                        updatecols.Add(this.GetCols(jsoncols_upd, _row));
                    else
                        insertcols.Add(this.GetCols(jsoncols_ins, _row));
                }
                if (insertcols.Count > 0)
                {
                    sqlData.JsonColoumsInsert.Add(new JsonTable
                    {
                        TableName = kp.Key,
                        Rows = insertcols
                    });
                }
                if (updatecols.Count > 0)
                {
                    sqlData.JsonColoumsUpdate.Add(new JsonTable
                    {
                        TableName = kp.Key,
                        Rows = updatecols
                    });
                }
            }
        }

        //generate insert obj and update object
        private JsonColVal GetCols(JsonColVal col, SingleRow row)
        {
            foreach (SingleColumn _cols in row.Columns)
            {
                col.Add(_cols.Name, _cols.Value);
            }
            return col;
        }

        public GetMobMenuResonse Get(GetMobMenuRequest request)
        {
            GetMobMenuResonse resp = new GetMobMenuResonse();
            try
            {
                string sql = @"SELECT 
	                                EA.id,
	                                EA.applicationname,
	                                EA.app_icon,
	                                EA.application_type 
                                FROM 
	                                eb_applications EA
                                LEFT JOIN 
	                                eb_objects2application EOA
                                ON 
	                                EA.id = EOA.app_id 
                                WHERE 
	                                EOA.obj_id = ANY(string_to_array(:ids,',')::int[]) 
                                AND 
	                                EA.eb_del = 'F'
                                AND
									EOA.eb_del = 'F';";

                User UserObject = this.Redis.Get<User>(request.UserAuthId);
                string[] Ids = UserObject.GetAccessIds(request.LocationId);

                DbParameter[] parameters =
                {
                    this.EbConnectionFactory.DataDB.GetNewParameter("ids", EbDbTypes.String, String.Join(",",Ids)),
                };

                EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql,parameters);
                foreach (EbDataRow row in dt.Rows)
                {
                    resp.Applications.Add(new AppDataToMob
                    {
                        AppId = Convert.ToInt32(row["id"]),
                        AppName = row["applicationname"].ToString(),
                        AppIcon = row["app_icon"].ToString(),
                    });
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
                Console.WriteLine("EXCEPTION AT GetMobMenuRequest " + e.Message);
            }
            return resp;
        }

        //objectlist for mobile
        public ObjectListToMob Get(ObjectListToMobRequest request)
        {
            Dictionary<int, List<ObjWrap>> dict = new Dictionary<int, List<ObjWrap>>();

            try
            {
                User UserObject = this.Redis.Get<User>(request.UserAuthId);
                string[] PermIds = UserObject.GetAccessIds(request.LocationId);

                string Sql = @"SELECT
                                   EO.id, EO.obj_type, EO.obj_name,
                                   EOV.version_num, 
                                   EOV.refid,
                                   display_name
                                FROM
   	                                eb_objects EO, eb_objects_ver EOV, eb_objects_status EOS, eb_objects2application EO2A 
                                WHERE
   	                                EOV.eb_objects_id = EO.id	
                                AND EO2A.app_id = :appid	      			    
                                AND EOS.eb_obj_ver_id = EOV.id 
                                AND EO2A.obj_id = EO.id
                                AND EO2A.eb_del = 'F'
                                AND EOS.status = 3 
                                AND COALESCE( EO.eb_del, 'F') = 'F'
                                AND EOS.id = ANY( Select MAX(id) from eb_objects_status EOS Where EOS.eb_obj_ver_id = EOV.id );";

                DbParameter[] parameters = {
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("appid",EbDbTypes.Int32,request.AppId)
                };

                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(Sql, parameters);

                foreach (EbDataRow dr in dt.Rows)
                {
                    int _ObjType = Convert.ToInt32(dr["obj_type"]);
                    EbObjectType _EbObjType = (EbObjectType)_ObjType;

                    string _ObjId = dr["id"].ToString();

                    if (!_EbObjType.IsUserFacing && !PermIds.Contains(_ObjId))
                        continue;

                    if (!dict.ContainsKey(_ObjType))
                        dict.Add(_ObjType, new List<ObjWrap>());

                    dict[_ObjType].Add(new ObjWrap
                    {
                        Id = Convert.ToInt32(dr["id"]),
                        EbObjectType = Convert.ToInt32(dr["obj_type"]),
                        Refid = dr["refid"].ToString(),
                        EbType = _EbObjType.Name,
                        DisplayName = dr["display_name"].ToString(),
                        VersionNumber = dr["version_num"].ToString()
                    });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception at sidebar user mobile req ::" + ex.Message);
            }

            return new ObjectListToMob { ObjectTypes = dict };
        }

        public EbObjectToMobResponse Get(EbObjectToMobRequest request)
        {
            EbObjectToMobResponse response = new EbObjectToMobResponse();

            try
            {
                var resp = (EbObjectParticularVersionResponse)StudioServices.Get(new EbObjectParticularVersionRequest { RefId = request.RefId });

                if (!resp.Data.Any())
                    return null;

                response.ObjectWraper = resp.Data[0];

                if (resp.Data[0].EbObjectType == EbObjectTypes.Report)
                {
                    EbReport reportobj = EbSerializers.Json_Deserialize<EbReport>(resp.Data[0].Json);
                    if (!string.IsNullOrEmpty(reportobj.DataSourceRefId))
                    {
                        ObjWrapperInt dr = this.GetObjectByVer(reportobj.DataSourceRefId);

                        if (string.IsNullOrEmpty((dr.EbObj as EbDataReader).FilterDialogRefId))
                        {
                            ReportService rep_service = base.ResolveService<ReportService>();
                            ReportRenderResponse stream = rep_service.Get(new ReportRenderRequest
                            {
                                Refid = request.RefId,
                                SolnId = request.SolnId,
                                UserId = request.UserId,
                                ReadingUser = request.User,
                                RenderingUser = request.User
                            });

                            response.ReportResult = stream.ReportBytea;
                        }
                    }

                }
                else if (resp.Data[0].EbObjectType == EbObjectTypes.TableVisualization)
                {
                    EbTableVisualization tv = EbSerializers.Json_Deserialize<EbTableVisualization>(resp.Data[0].Json);

                    if (!string.IsNullOrEmpty(tv.DataSourceRefId))
                    {
                        ObjWrapperInt dr = this.GetObjectByVer(tv.DataSourceRefId);

                        if(string.IsNullOrEmpty((dr.EbObj as EbDataReader).FilterDialogRefId))
                        {
                            response.TableResult = this.EbConnectionFactory.DataDB.DoQueries((dr.EbObj as EbDataReader).Sql);
                        }
                    }
                }
            }
            catch(Exception e)
            {
                Console.WriteLine("EXCEPTION AT EbObjectToMobResponse" + e.Message);
                Console.WriteLine(e.StackTrace);
            }
            
            return response;
        }
    }
}
