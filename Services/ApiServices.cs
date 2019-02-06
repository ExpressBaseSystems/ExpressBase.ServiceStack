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
using Npgsql;
using ServiceStack.Redis;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    public class ApiServices : EbBaseService
    {
        EbObjectService StudioServices { set; get; }

        DataSourceService DSService { set; get; }

        PdfToEmailService EmailService { set; get; }

        Dictionary<string, object> GlobalParams { set; get; }

        Dictionary<string, object> TempParams { set; get; }

        public EbApi Api { set; get; }

        public string Message { set; get; }

        public ApiServices(IEbConnectionFactory _dbf) : base(_dbf)
        {
            StudioServices = base.ResolveService<EbObjectService>();
            DSService = base.ResolveService<DataSourceService>();
            EmailService = base.ResolveService<PdfToEmailService>();
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

        private JsonColVal GetCols(JsonColVal col, SingleRow row)
        {
            foreach (SingleColumn _cols in row.Columns)
            {
                col.Add(_cols.Name, _cols.Value);
            }
            return col;
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
            this.GlobalParams = request.Data;
            var o = new object();
            int r_count = 0;
            string message = string.Empty;
            int step = 0;
            this.Api = this.Get(new ApiByNameRequest { Name = request.Name, Version = request.Version }).Api;
            try
            {
                if (Api != null)
                {
                    r_count = this.Api.Resources.Count;
                    while (step < r_count)
                    {
                        this.Api.Resources[step].Result = this.GetResult(this.Api.Resources[step], step);
                        step++;
                    }
                    return new ApiResponse { Result = this.Api.Resources[step - 1].Result, Message = Message };
                }
                else
                    return new ApiResponse { Message = "Api does not exist!", Result = null };
            }
            catch (Exception e)
            {
                return new ApiResponse { Result = null, Message = e.Message };
            }
        }

        private object GetResult(EbApiWrapper resource, int index)
        {
            ObjWrapperInt o_wrapper = null;
            ResultWrapper res = new ResultWrapper();
            List<Param> i_param = null;

            if (resource is EbSqlReader)
            {
                o_wrapper = this.GetObjectByVer(resource.Refid);
                i_param = this.GetInputParams(o_wrapper.EbObj, o_wrapper.ObjectType, index);
                res.Result = this.ExcDataReader(o_wrapper, i_param);
            }
            else if (resource is EbSqlWriter)
            {
                o_wrapper = this.GetObjectByVer(resource.Refid);
                i_param = this.GetInputParams(o_wrapper.EbObj, o_wrapper.ObjectType, index);
                res.Result = this.ExcDataWriter(o_wrapper, i_param);
            }
            else if (resource is EbSqlFunc)
            {
                o_wrapper = this.GetObjectByVer(resource.Refid);
                i_param = this.GetInputParams(o_wrapper.EbObj, o_wrapper.ObjectType, index);
                res.Result = this.ExcSqlFunction(o_wrapper, i_param);
            }
            else if (resource is EbEmailNode)
            {
                o_wrapper = this.GetObjectByVer(resource.Refid);
                i_param = this.GetInputParams(o_wrapper.EbObj, o_wrapper.ObjectType, index);
                res.Result = this.ExcEmail(o_wrapper, i_param, resource.Refid);
            }
            else
            {

            }
            return res.Result;
        }

        private object ExcDataReader(ObjWrapperInt wrapper, List<Param> i_param)
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
                this.Message = "Success";
            }
            catch(Exception e)
            {
                throw new ApiException(e.Message);
            }
            return dt;
        }

        private void FillParams(List<Param> _param)
        {
            foreach (Param p in _param)
            {
                if (this.GlobalParams.ContainsKey(p.Name))
                    p.Value = this.GlobalParams[p.Name].ToString();
                else if (this.TempParams != null && this.TempParams.ContainsKey(p.Name))
                    p.Value = this.TempParams[p.Name].ToString();

                if (string.IsNullOrEmpty(p.Value))
                {
                    throw new ApiException("Parameter " + p.Name + " must be set!");
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

        private bool ExcEmail(ObjWrapperInt wrapper, List<Param> i_param,string refid)
        {
            bool stat = false;
            this.FillParams(i_param);
            try
            {
                EmailService.Post(new PdfCreateServiceMqRequest
                {
                    Params = i_param,
                    ObjId = Convert.ToInt32(refid.Split(CharConstants.DASH)[3])
                });
                stat = true;
                this.Message = "Mail sent";
            }
            catch(Exception e) {
                stat = false;
                throw new ApiException(e.Message);
            }
            return stat;
        }

        private List<Param> GetInputParams(object ar, int obj_type, int step_c)
        {
            List<Param> p = null;
            if (ar is EbDataReader || ar is EbDataWriter || ar is EbSqlFunction)
            {
                p = GetSqlParams(ar as EbDataSourceMain, obj_type);
            }
            else if (ar is EbEmailTemplate)
            {
                p = this.GetEmailParams(ar as EbEmailTemplate);
            }
            else
                return null;

            if (step_c != 0)
            {
                this.SetOutParams(p, step_c);
            }
            return p;
        }

        private List<Param> GetSqlParams(EbDataSourceMain o, int obj_type)
        {
            bool isFilter = false;
            if (o is EbDataReader)
            {
                if (!string.IsNullOrEmpty((o as EbDataReader).FilterDialogRefId))
                    isFilter = true;
            }

            if (!isFilter)
            {
                if ((o.InputParams != null) && (o.InputParams.Any()))
                    return o.InputParams;
                else
                    return SqlHelper.GetSqlParams(o.Sql, obj_type);
            }
            else
            {
                (o as EbDataReader).AfterRedisGet(Redis as RedisClient);
                List<Param> p = new List<Param>();
                foreach (EbControl ctrl in (o as EbDataReader).FilterDialog.Controls)
                {
                    p.Add(new Param
                    {
                        Name = ctrl.Name,
                        Type = ((int)ctrl.EbDbType).ToString(),
                    });
                }
                return p;
            }
        }

        private void SetOutParams(List<Param> p, int step)
        {
            Dictionary<string, object> temp = new Dictionary<string, object>();
            if (this.Api.Resources[step - 1].Result != null)
            {
                var o = this.Api.Resources[step - 1].GetOutParams(p);

                foreach (Param pr in (o as List<Param>))
                {
                    if (!temp.ContainsKey(pr.Name))
                    {
                        temp.Add(pr.Name, pr.ValueTo);
                    }
                }
            }
            this.TempParams = temp;
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
                    p = p.Merge(this.GetSqlParams(ob.EbObj as EbDataSourceMain, ob.ObjectType)).ToList();
                }
            }
            if (!string.IsNullOrEmpty(enode.DataSourceRefId))
            {
                ObjWrapperInt ob = this.GetObjectByVer(enode.DataSourceRefId);
                p = p.Merge(this.GetSqlParams(ob.EbObj as EbDataSourceMain, ob.ObjectType)).ToList();
            }
            return p;
        }
    }

    public static class ApiExtensions
    {
        public static List<Param> Merge(this List<Param> to, List<Param> from)
        {
            foreach (Param p1 in from)
            {
                if (to.Find(x => x.Name == p1.Name) == null)
                    to.Add(p1);
            }
            return to;
        }
    }
}
