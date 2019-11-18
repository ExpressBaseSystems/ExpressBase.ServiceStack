using ExpressBase.Common;
using ExpressBase.Common.Application;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    public class DevRelatedServices : EbBaseService
    {
        public DevRelatedServices(IEbConnectionFactory _dbf, IMessageProducer _mqp, IMessageQueueClient _mqc) : base(_dbf, _mqp, _mqc) { }

        public GetApplicationResponse Get(GetApplicationRequest request)
        {
            GetApplicationResponse resp = new GetApplicationResponse();

            using (var con = EbConnectionFactory.DataDB.GetNewConnection())
            {
                string sql = "";
                if (request.Id > 0)
                {
                    sql = "SELECT id, applicationname, description, application_type, app_icon, app_settings FROM eb_applications WHERE id = :id AND eb_del = 'F'";
                }
                else
                {
                    sql = "SELECT id, applicationname FROM eb_applications WHERE eb_del = 'F'";
                }
                DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Id) };

                var dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql, parameters);

                Dictionary<string, object> Dict = new Dictionary<string, object>();
                if (request.Id <= 0)
                {
                    foreach (var dr in dt.Rows)
                    {
                        Dict.Add(dr[0].ToString(), dr[1]);
                    }

                    resp.Data = Dict;
                }
                else
                {
                    AppWrapper _app = new AppWrapper
                    {
                        Id = Convert.ToInt32(dt.Rows[0][0]),
                        Name = dt.Rows[0][1].ToString(),
                        Description = dt.Rows[0][2].ToString(),
                        AppType = Convert.ToInt32(dt.Rows[0][3]),
                        Icon = dt.Rows[0][4].ToString(),
                        AppSettings = dt.Rows[0][5]
                    };
                    resp.AppInfo = _app;
                }

            }
            return resp;
        }

        public GetAllApplicationResponse Get(GetAllApplicationRequest request)
        {
            GetAllApplicationResponse resp = new GetAllApplicationResponse();
            try
            {
                string sql = "SELECT id,applicationname,app_icon,application_type,description FROM eb_applications WHERE eb_del='F'";
                var ds = this.EbConnectionFactory.ObjectsDB.DoQuery(sql);
                List<AppWrapper> list = new List<AppWrapper>();
                foreach (EbDataRow dr in ds.Rows)
                {
                    list.Add(new AppWrapper
                    {
                        Id = Convert.ToInt32(dr[0]),
                        Name = dr[1].ToString(),
                        Icon = dr[2].ToString(),
                        AppType = Convert.ToInt32(dr[3]),
                        Description = dr[4].ToString()
                    });
                }
                resp.Data = list;
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception" + e.Message);
            }
            return resp;
        }

        public GetObjectsByAppIdResponse Get(GetObjectsByAppIdRequest request)
        {
            GetObjectsByAppIdResponse resp = new GetObjectsByAppIdResponse();
            try
            {
                string sql = @" SELECT applicationname,description,app_icon,application_type, app_settings FROM eb_applications WHERE id=:appid AND  eb_del = 'F';
				                SELECT 
				                     EO.id, EO.obj_type, EO.obj_name, EO.obj_desc, EO.display_name
				                FROM
				                     eb_objects EO
				                INNER JOIN
				                     eb_objects2application EO2A
				                ON
				                     EO.id = EO2A.obj_id
				                WHERE 
				                    EO2A.app_id=:appid
                                AND
                                    COALESCE(EO2A.eb_del, 'F') = 'F'
                                AND 
									EO.eb_del = 'F'
				                ORDER BY
				                    EO.obj_type;";

                DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("appid", EbDbTypes.Int32, request.Id) };

                var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

                int appType = Convert.ToInt32(dt.Tables[0].Rows[0][3]);
                object appStng = null;
                if (appType == 3)//if bot app
                {
                    appStng = JsonConvert.DeserializeObject<EbBotSettings>(dt.Tables[0].Rows[0][4].ToString());
                }

                resp.AppInfo = new AppWrapper
                {
                    Id = request.Id,
                    Name = dt.Tables[0].Rows[0][0].ToString(),
                    Description = dt.Tables[0].Rows[0][1].ToString(),
                    Icon = dt.Tables[0].Rows[0][2].ToString(),
                    AppType = appType,
                    AppSettings = appStng
                };
                resp.ObjectsCount = dt.Tables[1].Rows.Count;

                Dictionary<int, TypeWrap> _types = new Dictionary<int, TypeWrap>();
                foreach (EbDataRow dr in dt.Tables[1].Rows)
                {
                    var typeId = Convert.ToInt32(dr[1]);

                    var ___otyp = (EbObjectType)Convert.ToInt32(dr[1]);

                    if (___otyp.IsAvailableIn(request.AppType))
                    {
                        if (!_types.Keys.Contains<int>(typeId))
                            _types.Add(typeId, new TypeWrap { Objects = new List<ObjWrap>() });

                        _types[typeId].Objects.Add(new ObjWrap
                        {
                            Id = (dr[0] != null) ? Convert.ToInt32(dr[0]) : 0,
                            EbObjectType = (dr[1] != null) ? Convert.ToInt32(dr[1]) : 0,
                            ObjName = dr[2].ToString(),
                            Description = dr[3].ToString(),
                            EbType = ___otyp.ToString(),
                            DisplayName = dr[4].ToString()
                        });
                    }
                }
                resp.Data = _types;
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception" + e.Message);
            }

            return resp;
        }

        public object Get(GetObjectRequest request)
        {
            var Query1 = "SELECT EO.id, EO.obj_type, EO.obj_name,EO.obj_desc,EO.applicationid FROM eb_objects EO ORDER BY EO.obj_type";
            var ds = this.EbConnectionFactory.ObjectsDB.DoQuery(Query1);
            Dictionary<int, TypeWrap> _types = new Dictionary<int, TypeWrap>();
            try
            {
                foreach (EbDataRow dr in ds.Rows)
                {
                    var typeId = Convert.ToInt32(dr[1]);

                    if (!_types.Keys.Contains<int>(typeId))
                        _types.Add(typeId, new TypeWrap { Objects = new List<ObjWrap>() });

                    var ___otyp = (EbObjectType)Convert.ToInt32(dr[1]);

                    _types[typeId].Objects.Add(new ObjWrap
                    {
                        Id = Convert.ToInt32(dr[0]),
                        EbObjectType = Convert.ToInt32(dr[1]),
                        ObjName = dr[2].ToString(),
                        Description = dr[3].ToString(),
                        EbType = ___otyp.ToString(),
                        AppId = Convert.ToInt32(dr[4])
                    });
                }
            }
            catch (Exception ee)
            {
                Console.WriteLine("Exception: " + ee.ToString());
            }
            return new GetObjectResponse { Data = _types };
        }

        public CreateApplicationResponse Post(CreateApplicationRequest request)
        {
            CreateApplicationResponse resp = new CreateApplicationResponse();
            UniqueApplicationNameCheckResponse uniq_appnameresp;
            List<DbParameter> parameters = new List<DbParameter>();
            EbDataTable dt;
            if (request.AppId <= 0)
            {
                int c = 0;
                do
                {
                    c++;
                    uniq_appnameresp = Get(new UniqueApplicationNameCheckRequest { AppName = request.AppName });
                    if (!uniq_appnameresp.IsUnique)
                        request.AppName = request.AppName + "(" + c + ")";
                }
                while (!uniq_appnameresp.IsUnique);
            }

            try
            {
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("applicationname", EbDbTypes.String, request.AppName));
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("apptype", EbDbTypes.Int32, request.AppType));
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("description", EbDbTypes.String, request.Description));
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("appicon", EbDbTypes.String, request.AppIcon));

                if (request.AppId <= 0)//new mode
                {
                    dt = this.EbConnectionFactory.ObjectsDB.DoQuery(EbConnectionFactory.ObjectsDB.EB_CREATEAPPLICATION_DEV, parameters.ToArray());
                    resp.Id = Convert.ToInt32(dt.Rows[0][0]);
                }
                else//edit mode
                {
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("appid", EbDbTypes.Int32, request.AppId));
                    int st = this.EbConnectionFactory.ObjectsDB.DoNonQuery(EbConnectionFactory.ObjectsDB.EB_EDITAPPLICATION_DEV, parameters.ToArray());
                    if (st > 0)
                    {
                        resp.Id = request.AppId;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("exception:" + e.Message);
                resp.Id = 0;
            }
            return resp;
        }

        public GetTbaleSchemaResponse Get(GetTableSchemaRequest request)
        {
            Dictionary<string, List<Coloums>> Dict = new Dictionary<string, List<Coloums>>();
            string query = EbConnectionFactory.DataDB.EB_GETTABLESCHEMA;

            var res = this.EbConnectionFactory.DataDB.DoQuery(query);
            string key = "";
            foreach (EbDataRow dr in res.Rows)
            {
                key = dr[0] as string;
                if (!Dict.ContainsKey(key))
                    Dict.Add(key, new List<Coloums> { });

                Dict[key].Add(new Coloums
                {
                    cname = dr[1] as string,
                    type = dr[2] as string,
                    constraints = dr[3] as string,
                    foreign_tnm = dr[4] as string,
                    foreign_cnm = dr[5] as string
                });
            }
            return new GetTbaleSchemaResponse { Data = Dict };
        }

        public SaveAppSettingsResponse Any(SaveAppSettingsRequest request)
        {
            string sql = "UPDATE eb_applications SET app_settings = :newsettings WHERE id = :appid AND application_type = :apptype AND eb_del='F';";
            DbParameter[] parameters = new DbParameter[] {
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("appid", EbDbTypes.Int32, request.AppId),
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("apptype", EbDbTypes.Int32, request.AppType),
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("newsettings", EbDbTypes.String, request.Settings)
            };
            this.Redis.Set<EbBotSettings>(string.Format("{0}-{1}_app_settings", request.SolnId, request.AppId), JsonConvert.DeserializeObject<EbBotSettings>(request.Settings));
            return new SaveAppSettingsResponse()
            {
                ResStatus = this.EbConnectionFactory.ObjectsDB.DoNonQuery(sql, parameters)
            };
        }

        public UniqueApplicationNameCheckResponse Get(UniqueApplicationNameCheckRequest request)
        {
            DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("name", EbDbTypes.String, request.AppName) };
            EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery("SELECT id FROM eb_applications WHERE applicationname = :name ;", parameters);
            bool _isunique = (dt.Rows.Count > 0) ? false : true;
            return new UniqueApplicationNameCheckResponse { IsUnique = _isunique };
        }

        public string Get(GetDefaultMapApiKeyFromConnectionRequest request)
        {
            string _apikey = (this.EbConnectionFactory.MapConnection != null) ? this.EbConnectionFactory.MapConnection.GetDefaultApikey() : string.Empty;
            return _apikey;
        }

        public DeleteAppResponse Post(DeleteAppRequest request)
        {
            string q = @"UPDATE eb_applications SET eb_del = 'T' WHERE id = :appid";
            DeleteAppResponse resp = new DeleteAppResponse();
            try
            {
                DbParameter[] parameters = new DbParameter[]
                {
                    this.EbConnectionFactory.DataDB.GetNewParameter("appid",EbDbTypes.Int32,request.AppId)
                };
                int st = this.EbConnectionFactory.DataDB.DoNonQuery(q, parameters);
                if (st > 0)
                    resp.Status = true;
                else
                    resp.Status = false;
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
                Console.WriteLine(e.Message);
                resp.Status = false;
            }
            return resp;
        }
    }
}

