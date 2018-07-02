using ExpressBase.Common;
using ExpressBase.Common.Application;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    public class DevRelatedServices : EbBaseService
    {
        public DevRelatedServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        public GetApplicationResponse Get(GetApplicationRequest request)
        {
            GetApplicationResponse resp = new GetApplicationResponse();

            using (var con = EbConnectionFactory.DataDB.GetNewConnection())
            {
                string sql = "";
                if (request.id > 0)
                {
                    sql = "SELECT * FROM eb_applications WHERE id = :id";

                }
                else
                {
                    sql = "SELECT id, applicationname FROM eb_applications";
                }
                DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.id) };

                var dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql, parameters);

                Dictionary<string, object> Dict = new Dictionary<string, object>();
                if (request.id <= 0)
                {
                    foreach (var dr in dt.Rows)
                    {
                        Dict.Add(dr[0].ToString(), dr[1]);
                    }
                }
                else
                {
                    Dict.Add("applicationname", dt.Rows[0][0]);
                    Dict.Add("description", dt.Rows[0][1]);
                }
                resp.Data = Dict;

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
				//string sql = @" SELECT applicationname,description,app_icon,application_type, app_settings FROM eb_applications WHERE id=:appid;
				//                SELECT 
				//                     EO.id, EO.obj_type, EO.obj_name, EO.obj_desc
				//                FROM
				//                     eb_objects EO
				//                INNER JOIN
				//                     eb_objects2application EO2A
				//                ON
				//                     EO.id = EO2A.obj_id
				//                WHERE 
				//                 EO2A.app_id=:appid
				//                ORDER BY
				//                    EO.obj_type;";

				string sql = @" SELECT applicationname,description,app_icon,application_type FROM eb_applications WHERE id=:appid;
                              SELECT 
                                     EO.id, EO.obj_type, EO.obj_name, EO.obj_desc
                                FROM
                                     eb_objects EO
                                INNER JOIN
                                     eb_objects2application EO2A
                                ON
                                     EO.id = EO2A.obj_id
                                WHERE 
	                                EO2A.app_id=:appid
                                ORDER BY
                                    EO.obj_type;";// del this qry, uncomment above qry after app_settings col created in all using solutions
				DbParameter[] parameters = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("appid", EbDbTypes.Int32, request.Id) };

                var dt = this.EbConnectionFactory.ObjectsDB.DoQueries(sql, parameters);

				int appType = Convert.ToInt32(dt.Tables[0].Rows[0][3]);
				object appStng = null;
				if (appType == 3)//if bot app
				{
					//appStng = JsonConvert.DeserializeObject<EbBotSettings>(dt.Tables[0].Rows[0][4].ToString());//uncomment after app_settings added
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
                            EbType = ___otyp.ToString()
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
            string DbName = request.Sid;
            CreateApplicationResponse resp;
            using (var con = this.EbConnectionFactory.DataDB.GetNewConnection(DbName.ToLower()))
            {
                con.Open();
                if (!string.IsNullOrEmpty(request.AppName))
                {
                    string sql = "INSERT INTO eb_applications (applicationname,application_type, description,app_icon) VALUES (:applicationname,:apptype, :description,:appicon) RETURNING id";

                    var cmd = EbConnectionFactory.DataDB.GetNewCommand(con, sql);
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("applicationname", EbDbTypes.String, request.AppName));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("apptype", EbDbTypes.Int32, request.AppType));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("description", EbDbTypes.String, request.Description));
                    cmd.Parameters.Add(EbConnectionFactory.ObjectsDB.GetNewParameter("appicon", EbDbTypes.String, request.AppIcon));
                    var res = cmd.ExecuteScalar();
                    resp = new CreateApplicationResponse() { id = Convert.ToInt32(res) };
                }
                else
                    resp = new CreateApplicationResponse() { id = 0 };
            }
            return resp;
        }

        public CreateApplicationResponse Post(CreateApplicationDevRequest request)
        {
            CreateApplicationResponse resp;
            try
            {
                string sql = "INSERT INTO eb_applications (applicationname,application_type, description,app_icon) VALUES (:applicationname,:apptype, :description,:appicon)";

                DbParameter[] parameters = {
                    this.EbConnectionFactory.DataDB.GetNewParameter("applicationname", EbDbTypes.String, request.AppName),
                    this.EbConnectionFactory.DataDB.GetNewParameter("apptype", EbDbTypes.Int32, request.AppType),
                    this.EbConnectionFactory.DataDB.GetNewParameter("description", EbDbTypes.String, request.Description),
                    this.EbConnectionFactory.DataDB.GetNewParameter("appicon", EbDbTypes.String, request.AppIcon)
                };
                var res = this.EbConnectionFactory.DataDB.DoNonQuery(sql, parameters);

                resp = new CreateApplicationResponse() { id = Convert.ToInt32(res) };//returning row affected

            }
            catch (Exception e)
            {
                Console.WriteLine("exception:" + e.Message);
                resp = new CreateApplicationResponse() { id = 0 };
            }

            return resp;
        }

        public GetTbaleSchemaResponse Get(GetTableSchemaRequest request)
        {
            Dictionary<string, List<Coloums>> Dict = new Dictionary<string, List<Coloums>>();
            string query = @"
               SELECT 
              ACols.*,
                 BCols.foreign_table_name,
                    BCols.foreign_column_name 
            FROM
                    (SELECT 
                        TCols.*, CCols.constraint_type FROM
                            (SELECT
                             T.table_name, C.column_name, C.data_type
                            FROM 
                                information_schema.tables T,
                             information_schema.columns C
                            WHERE
                              T.table_name = C.table_name AND
                                 T.table_schema='public') TCols
                            LEFT JOIN
                            (SELECT 
                               TC.table_name,TC.constraint_type,KCU.column_name 
                             FROM
                              information_schema.table_constraints TC,
                              information_schema.key_column_usage KCU
                             WHERE
                              TC.constraint_name=KCU.constraint_name AND
                              (TC.constraint_type = 'PRIMARY KEY' OR TC.constraint_type = 'FOREIGN KEY') AND
                              TC.table_schema='public') CCols
                             ON 
                             CCols.table_name=TCols.table_name AND
                                CCols.column_name=TCols.column_name) ACols
             LEFT JOIN
                        (SELECT
                     tc.constraint_name, tc.table_name, kcu.column_name, 
                   ccu.table_name AS foreign_table_name,
                      ccu.column_name AS foreign_column_name 
               FROM 
             information_schema.table_constraints AS tc 
                  JOIN 
                       information_schema.key_column_usage AS kcu
                        ON 
                       tc.constraint_name = kcu.constraint_name
                     JOIN  
                       information_schema.constraint_column_usage AS ccu
                        ON 
                       ccu.constraint_name = tc.constraint_name
                        WHERE 
                       constraint_type = 'FOREIGN KEY' AND tc.table_schema='public') BCols
                     ON
                      ACols.table_name=BCols.table_name AND  ACols.column_name=BCols.column_name
                ORDER BY
                 table_name, column_name";

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
            return new GetTbaleSchemaResponse { Data = Dict};
        }

		public SaveAppSettingsResponse Any(SaveAppSettingsRequest request)
		{
			string sql = "UPDATE eb_applications SET app_settings = :newsettings WHERE id = :appid AND application_type = :apptype AND eb_del='F';";
			DbParameter[] parameters = new DbParameter[] {
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("appid", EbDbTypes.Int32, request.AppId),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("apptype", EbDbTypes.Int32, request.AppType),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("newsettings", EbDbTypes.String, request.Settings)
			};
			this.Redis.Set<EbBotSettings>(string.Format("{0}-{1}_app_settings", request.TenantAccountId, request.AppId), JsonConvert.DeserializeObject<EbBotSettings>(request.Settings));
			return new SaveAppSettingsResponse()
			{
				ResStatus = this.EbConnectionFactory.ObjectsDB.DoNonQuery(sql, parameters)
			};
		}
	}
}

