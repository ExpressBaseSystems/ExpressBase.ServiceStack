using ExpressBase.Common;
using ExpressBase.Common.Connections;
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
using RestSharp;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Net.Http;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class ApiServices : EbBaseService
    {
        private EbObjectService StudioServices { set; get; }

        private DataSourceService DSService { set; get; }

        private PdfToEmailService EmailService { set; get; }

        private Dictionary<string, object> GlobalParams { set; get; }

        private Dictionary<string, object> TempParams { set; get; }

        private EbApi Api { set; get; }

        private ApiResponse ApiResponse { set; get; }

        private string SolutionId { set; get; }

        public ApiServices(IEbConnectionFactory _dbf) : base(_dbf)
        {
            this.StudioServices = base.ResolveService<EbObjectService>();
            this.DSService = base.ResolveService<DataSourceService>();
            this.EmailService = base.ResolveService<PdfToEmailService>();
            this.ApiResponse = new ApiResponse();
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

            List<Param> p = this.Get(new ApiReqJsonRequest
            {
                SolnId = request.SolutionId,
                Components = resp.Api.Resources
            }).Params;

            return new ApiMetaResponse { Params = p ,Name=request.Name,Version=request.Version};
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
            foreach(EbDataRow row in dt.Rows)
            {
                resp.AllMetas.Add(new EbObjectWrapper {Name = row[0].ToString(),VersionNumber=row["version_num"].ToString() });
            }

            return resp;
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
                    if (!string.IsNullOrEmpty(enode.AttachmentReportRefID))
                    {
                        ObjWrapperInt rep = this.GetObjectByVer(enode.AttachmentReportRefID);
                        EbReport o = (EbReport)rep.EbObj;
                        if (!string.IsNullOrEmpty(o.DataSourceRefId))
                        {
                            ObjWrapperInt ds = this.GetObjectByVer(o.DataSourceRefId);
                            p = p.Merge((ds.EbObj as EbDataSourceMain).GetParams(this.Redis as RedisClient));
                        }
                    }
                    if (!string.IsNullOrEmpty(enode.DataSourceRefId))
                    {
                        ObjWrapperInt ob = this.GetObjectByVer(enode.DataSourceRefId);
                        p = p.Merge((ob.EbObj as EbDataSourceMain).GetParams(this.Redis as RedisClient));
                    }
                }
                else if (r is EbConnectApi)
                {
                    ObjWrapperInt ob = this.GetObjectByVer(r.Reference);
                    p = p.Merge(this.Get(new ApiReqJsonRequest { Components = (ob.EbObj as EbApi).Resources }).Params);
                }
            }
            return new ApiReqJsonResponse { Params = p };
        }

        private JsonColVal GetCols(JsonColVal col, SingleRow row)
        {
            foreach (SingleColumn _cols in row.Columns)
            {
                col.Add(_cols.Name, _cols.Value);
            }
            return col;
        }

        public ApiResponse Post(ApiComponetRequest request)
        {
            try
            {
                this.GlobalParams = request.Params.Select(p => new { prop = p.Name, val = p.ValueTo })
                    .ToDictionary(x => x.prop, x => x.val as object);
                ObjWrapperInt ow = this.GetObjectByVer(request.Component.Reference);
                if (request.Component is EbSqlReader)
                    request.Component.Result = this.ExcDataReader(ow, request.Params, request.Component.RouteIndex);
                else if (request.Component is EbSqlWriter)
                    request.Component.Result = this.ExcDataWriter(ow, request.Params);
                else if (request.Component is EbSqlFunc)
                    request.Component.Result = this.ExcSqlFunction(ow, request.Params);
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

        private ObjWrapperInt GetObjectByVer(string refid)
        {
            EbObjectParticularVersionResponse resp = (EbObjectParticularVersionResponse)StudioServices.Get(new EbObjectParticularVersionRequest { RefId = refid });
            return new ObjWrapperInt
            {
                ObjectType = resp.Data[0].EbObjectType,
                EbObj = EbSerializers.Json_Deserialize(resp.Data[0].Json)
            };
        }

        public ApiResponse Any(ApiRequest request)
        {
            this.ApiResponse.Name = request.Name;
            this.ApiResponse.Version = request.Version;
            this.ApiResponse.Message.ExecutedOn = DateTime.UtcNow.ToString();
            var watch = new System.Diagnostics.Stopwatch(); watch.Start();
            this.SolutionId = request.SolnId;
            this.GlobalParams = request.Data;
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
                    watch.Stop();
                    this.ApiResponse.Result = this.Api.Resources[step - 1].GetResult();
                    this.ApiResponse.Message.Status = ApiConstants.SUCCESS;
                    this.ApiResponse.Message.ExecutionTime = watch.ElapsedMilliseconds.ToString() + " ms";
                    if (this.ApiResponse.Result != null)
                        this.ApiResponse.Message.ErrorCode = ApiErrorCode.Success;
                }
                else
                {
                    watch.Stop();
                    this.ApiResponse.Message.Status = "Unknown";
                    this.ApiResponse.Message.Description = string.Format(ApiConstants.API_NOTFOUND, request.Name);
                    this.ApiResponse.Message.ErrorCode = ApiErrorCode.NotFound;
                    this.ApiResponse.Message.ExecutionTime = watch.ElapsedMilliseconds.ToString() + " ms";
                }
            }
            catch (Exception e)
            {
                watch.Stop();
                this.ApiResponse.Message.ExecutionTime = watch.ElapsedMilliseconds.ToString() + " ms";
                this.ApiResponse.Message.Status = string.Format(ApiConstants.FAIL);
                Console.WriteLine(this.ApiResponse.Message.Status);
                Console.WriteLine(this.ApiResponse.Message.Description);
            }
            return this.ApiResponse;
        }

        private object GetResult(ApiResources resource, int index)
        {
            ObjWrapperInt o_wrapper = null;
            ResultWrapper res = new ResultWrapper();
            List<Param> i_param = null;

            if (resource is EbSqlReader)
            {
                o_wrapper = this.GetObjectByVer(resource.Reference);
                i_param = this.GetInputParams(o_wrapper.EbObj, index);
                res.Result = this.ExcDataReader(o_wrapper, i_param, index);
            }
            else if (resource is EbSqlWriter)
            {
                o_wrapper = this.GetObjectByVer(resource.Reference);
                i_param = this.GetInputParams(o_wrapper.EbObj, index);
                res.Result = this.ExcDataWriter(o_wrapper, i_param);
            }
            else if (resource is EbSqlFunc)
            {
                o_wrapper = this.GetObjectByVer(resource.Reference);
                i_param = this.GetInputParams(o_wrapper.EbObj, index);
                res.Result = this.ExcSqlFunction(o_wrapper, i_param);
            }
            else if (resource is EbEmailNode)
            {
                o_wrapper = this.GetObjectByVer(resource.Reference);
                i_param = this.GetInputParams(o_wrapper.EbObj, index);
                res.Result = this.ExcEmail(o_wrapper, i_param, resource.Reference, index);
            }
            else if (resource is EbProcessor)
            {
                res.Result = (resource as EbProcessor).Evaluate(this.Api.Resources[index - 1]);
            }
            else if (resource is EbConnectApi)
            {
                o_wrapper = this.GetObjectByVer(resource.Reference);
                if ((o_wrapper.EbObj as EbApi).Name.Equals(this.Api.Name))
                {
                    this.ApiResponse.Message.Description = string.Format(ApiConstants.CIRCULAR_REF, this.Api.Name) + ", " + string.Format(ApiConstants.DESCRPT_ERR, index, "ConnectApi", this.Api.Name);
                    throw new ApiException();
                }
                else
                {
                    i_param = this.GetInputParams(o_wrapper.EbObj, index);
                    res.Result = this.ExecuteConnectApi((resource as EbConnectApi), i_param);
                }
            }
            else if(resource is EbThirdPartyApi)
            {
                i_param = this.GetInputParams((resource as EbThirdPartyApi), index);
                res.Result = this.Exec3rdPartyApi((resource as EbThirdPartyApi),i_param);
            }
            return res.Result;
        }

        private object ExcDataReader(ObjWrapperInt wrapper, List<Param> i_param, int step_c)
        {
            this.FillParams(i_param);
            List<DbParameter> p = new List<DbParameter>();
            EbDataSet dt = null;
            try
            {
                foreach (Param pr in i_param)
                {
                    p.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(pr.Name, (EbDbTypes)Convert.ToInt32(pr.Type), pr.ValueTo));
                }
                dt = this.EbConnectionFactory.ObjectsDB.DoQueries((wrapper.EbObj as EbDataReader).Sql, p.ToArray());
                this.ApiResponse.Message.Description = ApiConstants.EXE_SUCCESS;
            }
            catch (Exception e)
            {
                this.ApiResponse.Message.Description = string.Format(ApiConstants.DESCRPT_ERR, step_c, "DataReader", wrapper.EbObj.Name);
                throw new ApiException(e.Message);
            }
            return dt;
        }

        private void FillParams(List<Param> _param)
        {
            foreach (Param p in _param)
            {
                if (this.TempParams != null && this.TempParams.ContainsKey(p.Name))
                    p.Value = this.TempParams[p.Name].ToString();
                else if (this.GlobalParams.ContainsKey(p.Name))
                    p.Value = this.GlobalParams[p.Name].ToString();
                else
                {
                    this.ApiResponse.Message.Description = string.Format(ApiConstants.UNSET_PARAM, p.Name);
                    throw new ApiException(((int)ApiErrorCode.Failed).ToString());
                }
                if (string.IsNullOrEmpty(p.Value))
                {
                    this.ApiResponse.Message.Description = string.Format(ApiConstants.UNSET_PARAM, p.Name);
                    throw new ApiException(((int)ApiErrorCode.Failed).ToString());
                }
            }
        }

        public object ExcDataWriter(ObjWrapperInt wrapper, List<Param> i_param)
        {
            this.FillParams(i_param);
            List<DbParameter> p = new List<DbParameter>();
            foreach (Param pr in i_param)
            {
                p.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(pr.Name, (EbDbTypes)Convert.ToInt32(pr.Type), pr.ValueTo));
            }
            return this.EbConnectionFactory.ObjectsDB.DoNonQuery((wrapper.EbObj as EbDataWriter).Sql, p.ToArray());
        }

        private object ExcSqlFunction(ObjWrapperInt wrapper, List<Param> i_param)
        {
            this.FillParams(i_param);
            return this.DSService.Post(new SqlFuncTestRequest { FunctionName = (wrapper.EbObj as EbSqlFunction).Name, Parameters = i_param });
        }

        private bool ExcEmail(ObjWrapperInt wrapper, List<Param> i_param, string refid, int step_c)
        {
            bool stat = false;
            this.FillParams(i_param);
            try
            {
                EmailService.Post(new EmailAttachmentMqRequest
                {
                    SolnId = this.SolutionId,
                    Params = i_param,
                    ObjId = Convert.ToInt32(refid.Split(CharConstants.DASH)[3])
                });
                stat = true;
                this.ApiResponse.Message.Description = string.Format(ApiConstants.MAIL_SUCCESS,
                                                        (wrapper.EbObj as EbEmailTemplate).To,
                                                        (wrapper.EbObj as EbEmailTemplate).Subject,
                                                        (wrapper.EbObj as EbEmailTemplate).Cc);
            }
            catch (Exception e)
            {
                stat = false;
                this.ApiResponse.Message.Description = string.Format(ApiConstants.DESCRPT_ERR, step_c, "Mail", wrapper.EbObj.Name);
                throw new ApiException(e.Message);
            }
            return stat;
        }

        private object ExecuteConnectApi(EbConnectApi c_api, List<Param> i_param)
        {
            ApiResponse resp = null;
            this.FillParams(i_param);
            Dictionary<string, object> d = i_param.Select(p => new { prop = p.Name, val = p.Value })
                   .ToDictionary(x => x.prop, x => x.val as object);
            try
            {
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
            catch (Exception e)
            {
                this.ApiResponse.Message.Description = string.Format(ApiConstants.DESCRPT_ERR, c_api.RouteIndex, "Api", c_api.RefName);
                throw new ApiException(e.Message);
            }
            return resp;
        }

        public string Exec3rdPartyApi(EbThirdPartyApi tpa,List<Param> param)
        {
            var uri = new Uri(tpa.Url);
            HttpResponseMessage response = null;
            using (var client = new HttpClient())
            {
                if (tpa.Headers != null && tpa.Headers.Any())
                {
                    foreach (RequestHeader header in tpa.Headers)
                    {
                        client.DefaultRequestHeaders.Add(header.Name, header.Value);
                    }
                }

                client.BaseAddress = new Uri(uri.GetLeftPart(System.UriPartial.Authority));
                if (tpa.Method == ApiMethods.POST)
                {
                    var parameters = param.Select(i => new { prop = i.Name, val = i.Value })
                            .ToDictionary(x => x.prop, x => x.val);
                    response = client.PostAsync(uri.PathAndQuery, new FormUrlEncodedContent(parameters)).Result;
                }
                else if (tpa.Method == ApiMethods.GET)
                {
                    response = client.GetAsync(uri.PathAndQuery).Result;
                }
            }
            return response.Content.ReadAsStringAsync().Result;
        }

        private List<Param> GetInputParams(object ar, int step_c)
        {
            List<Param> p = null;
            if (ar is EbDataReader || ar is EbDataWriter || ar is EbSqlFunction)
                p = (ar as EbDataSourceMain).GetParams(this.Redis as RedisClient);
            else if (ar is EbEmailTemplate)
                p = this.GetEmailParams(ar as EbEmailTemplate);
            else if (ar is EbApi)
                p = this.Get(new ApiReqJsonRequest { Components = (ar as EbApi).Resources }).Params;
            else if(ar is EbThirdPartyApi)
            {
                p = (ar as EbThirdPartyApi).Parameters.Select(i => new Param { Name = i.Name, Type = i.Type.ToString(), Value = i.Value })
                    .ToList();
            }
            else
                return null;

            if (step_c != 0)
                this.SetOutParams(p, step_c);
            return p;
        }

        private void SetOutParams(List<Param> p, int step)
        {
            if (this.Api.Resources[step - 1].Result != null)
            {
                List<Param> out_params = this.Api.Resources[step - 1].GetOutParams(p);
                this.TempParams = out_params.Select(i => new { prop = i.Name, val = i.ValueTo })
                        .ToDictionary(x => x.prop, x => x.val as object);

            }
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
    }
}
