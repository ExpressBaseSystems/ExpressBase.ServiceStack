using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
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

        public ApiServices(IEbConnectionFactory _dbf) : base(_dbf)
        {
            StudioServices = base.ResolveService<EbObjectService>();
            DSService = base.ResolveService<DataSourceService>();
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

        public ApiResponse Any(ApiRequest request)
        {
            var o = new object();
            int r_count = 0;
            string message = string.Empty;
            int step = 0;
            EbApi api_o = this.Get(new ApiByNameRequest { Name = request.Name, Version = request.Version }).Api;
            if (api_o != null)
            {
                r_count = api_o.Resources.Count;

                while (step < r_count)
                {
                    if (step == 0)
                        api_o.Resources[step].Result = this.GetResult(api_o.Resources[step], request.Data);
                    else if (step != r_count)
                    {
                        Dictionary<string, object> _data = this.ObjectToDict(api_o.Resources[step], api_o.Resources[step - 1].Result);
                        if (_data.Any())
                            api_o.Resources[step].Result = this.GetResult(api_o.Resources[step], _data);
                    }
                    step++;
                }
                return new ApiResponse { Result = api_o.Resources[step - 1].Result,Message="Success" };
            }
            else
                return new ApiResponse { Message = "Api does not exist!", Result = null };
        }

        private object GetResult(EbApiWrapper resource, Dictionary<string, object> data)
        {
            if (resource is EbSqlReader)
                return this.ExcDataReader(resource as EbSqlReader, data);
            else if (resource is EbSqlWriter)
                return this.ExcDataWriter(resource as EbSqlWriter, data);
            else if (resource is EbSqlFunc)
                return this.ExcSqlFunction(resource as EbSqlFunc, data);
            else
                return null;
        }

        public EbDataSet ExcDataReader(EbSqlReader reader, Dictionary<string, object> data)
        {
            var dr = this.GetSql_Params(reader, data);

            List<DbParameter> p = new List<DbParameter>();
            foreach (Param pr in dr.Parameter)
            {
                p.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(pr.Name, (EbDbTypes)Convert.ToInt32(pr.Type), pr.ValueTo));
            }
            return this.EbConnectionFactory.ObjectsDB.DoQueries(dr.Sql, p.ToArray());
        }

        public object ExcDataWriter(EbSqlWriter writer, Dictionary<string, object> data)
        {
            var dr = this.GetSql_Params(writer, data);

            List<DbParameter> p = new List<DbParameter>();
            foreach (Param pr in dr.Parameter)
            {
                p.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(pr.Name, (EbDbTypes)Convert.ToInt32(pr.Type), pr.ValueTo));
            }
            return this.EbConnectionFactory.ObjectsDB.DoNonQuery(dr.Sql, p.ToArray());
        }

        public object ExcSqlFunction(EbSqlFunc func, Dictionary<string, object> data)
        {
            var dr = this.GetSql_Params(func, data);
            return this.DSService.Post(new SqlFuncTestRequest { FunctionName = (dr.Object as EbSqlFunction).Name, Parameters = dr.Parameter });
        }

        public SqlParams GetSql_Params(EbApiWrapper o, Dictionary<string, object> data)
        {
            EbObjectParticularVersionResponse o_ver = this.GetObjectByVer(o.Refid);
            EbDataReader dr = EbSerializers.Json_Deserialize(o_ver.Data[0].Json);

            return new SqlParams
            {
                Sql = dr.Sql,
                Parameter = this.FillParams(GetParams(dr, o_ver.Data[0].EbObjectType), data),
                Object = dr
            };
        }

        private EbObjectParticularVersionResponse GetObjectByVer(string refid)
        {
            return (EbObjectParticularVersionResponse)StudioServices.Get(new EbObjectParticularVersionRequest { RefId = refid });
        }

        private List<Param> GetParams(EbDataSourceMain o, int obj_type)
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

        private List<Param> FillParams(List<Param> _param, Dictionary<string, object> data)
        {
            foreach (Param p in _param)
            {
                if (data.ContainsKey(p.Name))
                    p.Value = data[p.Name].ToString();
            }
            return _param;
        }

        private Dictionary<string, object> ObjectToDict(EbApiWrapper cur_step, object prev_step)
        {
            Dictionary<string, object> dict = new Dictionary<string, object>();

            if (prev_step is EbDataSet)
            {
                prev_step = (prev_step as EbDataSet);

                if (cur_step is EbSqlReader || cur_step is EbSqlWriter || cur_step is EbSqlFunc)
                {
                    var o = this.GetObjectByVer(cur_step.Refid);
                    EbDataReader dr = EbSerializers.Json_Deserialize(o.Data[0].Json);
                    List<Param> _param = this.GetParams(dr, o.Data[0].EbObjectType);

                    foreach (EbDataTable table in (prev_step as EbDataSet).Tables)
                    {
                        string[] c = _param.Select(item => item.Name).ToArray();
                        foreach (EbDataColumn cl in table.Columns)
                        {
                            if (c.Contains(cl.ColumnName))
                                dict.Add(cl.ColumnName, table.Rows[0][cl.ColumnIndex]);
                        }
                    }
                }
            }
            return dict;
        }
    }
}
