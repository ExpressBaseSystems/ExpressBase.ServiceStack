using ExpressBase.Common;
using ExpressBase.Common.Application;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
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
        public DevRelatedServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        public GetApplicationResponse Get(GetApplicationRequest request)
        {
            GetApplicationResponse resp = new GetApplicationResponse();

            using (var con = EbConnectionFactory.DataDB.GetNewConnection())
            {
                string sql = "";
                if (request.Id > 0)
                {
                    sql = "SELECT id, applicationname, description, application_type, app_icon, app_settings FROM eb_applications WHERE id = :id";

                }
                else
                {
                    sql = "SELECT id, applicationname FROM eb_applications";
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
                string sql = @" SELECT applicationname,description,app_icon,application_type, app_settings FROM eb_applications WHERE id=:appid;
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
                // DbParameter[] parameters= new DbParameter[];
                List<DbParameter> parameters = new List<DbParameter>();
                string sql;
                if (this.EbConnectionFactory.ObjectsDB.Vendor == DatabaseVendors.ORACLE)
                {
                    //sql = @"INSERT INTO eb_applications (applicationname,application_type, description,app_icon) VALUES (:applicationname,:apptype, :description,:appicon);
                    //     SELECT eb_applications_id_seq.CURRVAL FROM dual; ";
                    sql = "INSERT INTO eb_applications (applicationname,application_type, description,app_icon) VALUES (:applicationname,:apptype, :description,:appicon) returning id into :cur_id; ";
                    parameters.Add(this.EbConnectionFactory.DataDB.GetNewOutParameter("cur_id", EbDbTypes.Int32));
                }
                else
                {
                    sql = "INSERT INTO eb_applications (applicationname,application_type, description,app_icon) VALUES (:applicationname,:apptype, :description,:appicon) returning id; ";

                }
                //string sql = "INSERT INTO eb_applications (applicationname,application_type, description,app_icon) VALUES (:applicationname,:apptype, :description,:appicon) RETURNING id;";
                parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("applicationname", EbDbTypes.String, request.AppName));
                parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("apptype", EbDbTypes.Int32, request.AppType));
                parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("description", EbDbTypes.String, request.Description));
                parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("appicon", EbDbTypes.String, request.AppIcon));

                //var dt = this.EbConnectionFactory.DataDB.DoReturnId(sql, parameters.ToArray());

                //var dt = this.EbConnectionFactory.DataDB.DoQuery(sql, parameters.ToArray());

                var dt = this.EbConnectionFactory.DataDB.DoNonQuery(sql, parameters.ToArray());

                // resp = new CreateApplicationResponse() { id = Convert.ToInt32(dt.Rows[0][0]) };
                resp = new CreateApplicationResponse() { id = dt };

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
            this.Redis.Set<EbBotSettings>(string.Format("{0}-{1}_app_settings", request.TenantAccountId, request.AppId), JsonConvert.DeserializeObject<EbBotSettings>(request.Settings));
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

        public GetSurveyQueriesResponse Get(GetSurveyQueriesRequest request)
        {
            Dictionary<int,EbSurveyQuery> dict = new Dictionary<int, EbSurveyQuery>();

            string sql = @"SELECT Q.id,Q.query,Q.q_type,C.id,C.choice,C.score from eb_survey_queries Q 
                           INNER JOIN eb_query_choices C ON C.q_id = q.id WHERE C.eb_del = 'F';";

            var dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql);
            foreach (EbDataRow dr in dt.Rows)
            {
                int id = Convert.ToInt32(dr[0]);
                if (!dict.ContainsKey(id))
                {
                    dict.Add(id, new EbSurveyQuery
                    {
                        QuesId = id,
                        Question = dr[1] as string,
                        QuesType = Convert.ToInt32(dr[2]),
                        Choices = new List<QueryChoices>()
                    });

                    dict[id].Choices.Add(new QueryChoices
                    {
                        ChoiceId = Convert.ToInt32(dr[3]),
                        Choice = dr[4].ToString(),
                        Score = Convert.ToInt32(dr[5])
                    });
                }
                else
                {
                    dict[id].Choices.Add(new QueryChoices
                    {
                        ChoiceId = Convert.ToInt32(dr[3]),
                        Choice = dr[4].ToString(),
                        Score = Convert.ToInt32(dr[5])
                    });
                }
            }
                return new GetSurveyQueriesResponse { Data = dict };
        }

        public SurveyQuesResponse Post(SurveyQuesRequest request)
        {
            EbSurveyQuery ques = request.Query;
            SurveyQuesResponse resp = new SurveyQuesResponse();
            int c_count = 0;
            StringBuilder s = new StringBuilder();
            List<DbParameter> parameters = new List<DbParameter>();
            if(ques.QuesId > 0)
            {
                s.Append("UPDATE eb_survey_queries SET query=:query,q_type=:qtype WHERE id=:qid;");
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("query", EbDbTypes.String, ques.Question));
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("qtype", EbDbTypes.Int32, ques.QuesType));
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("qid", EbDbTypes.Int32, ques.QuesId));

                foreach (QueryChoices choice in ques.Choices)
                {
                    int count = c_count++;

                    if (choice.EbDel)
                    {
                        s.Append("UPDATE eb_query_choices SET eb_del = 'T' WHERE id=:choiceid"+ count + ";");
                        parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("choiceid" + count, EbDbTypes.Int32, choice.ChoiceId));
                    }
                    else if (choice.IsNew)
                    {
                        s.Append("INSERT INTO eb_query_choices(q_id,choice,score) VALUES(:questid"+ count + ",:choice"+ count + ",:chscore"+ count + ");");
                        parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("questid" + count, EbDbTypes.Int32, ques.QuesId));
                        parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("choice" + count, EbDbTypes.String, choice.Choice));
                        parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("chscore" + count, EbDbTypes.Int32, choice.Score));
                    }
                    else
                    {
                        s.Append("UPDATE eb_query_choices SET choice=:choice"+ count + ",score=:chscore"+ count + " WHERE id=:chid"+ count + ";");
                        parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("choice" + count, EbDbTypes.String, choice.Choice));
                        parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("chid" + count, EbDbTypes.Int32, choice.ChoiceId));
                        parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("chscore" + count, EbDbTypes.Int32, choice.Score));
                    }
                }
            }
            else
            {
                s.Append("INSERT INTO eb_survey_queries(query,q_type) VALUES(:query,:qtype);");
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("query", EbDbTypes.String, ques.Question));
                parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("qtype", EbDbTypes.Int32, ques.QuesType));

                s.Append("INSERT INTO eb_query_choices(q_id,choice,score) VALUES");

                foreach (QueryChoices choice in ques.Choices)
                {
                    int count = c_count++;
                    s.Append("(currval('eb_survey_queries_id_seq'),:choice" + count + ",:score"+ count + ")");

                    if (choice != ques.Choices.Last())
                        s.Append(",");
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("choice" + count, EbDbTypes.String, choice.Choice));
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("score" + count, EbDbTypes.Int32, choice.Score));
                }
                s.Append(";SELECT currval('eb_survey_queries_id_seq');");
            }
           
            var res = this.EbConnectionFactory.ObjectsDB.DoQueries(s.ToString(), parameters.ToArray());
            if (Convert.ToInt32(res.Tables[0].Rows[0][0]) > 0)
            {
                resp.Status = true;
                resp.Quesid = Convert.ToInt32(res.Tables[0].Rows[0][0]);
            }
            else
                resp.Status = false;
            return resp;
        }

		public ManageSurveyResponse Post(ManageSurveyRequest request)
		{
			Eb_Survey surveyObj = new Eb_Survey() { Id = 0};
			List<Eb_SurveyQuestion> questionList = new List<Eb_SurveyQuestion>();
			string qryStr = @"SELECT Q.id, Q.query, Q.q_type FROM eb_survey_queries Q;";

            if (request.Id > 0)
			{
				qryStr += @"SELECT S.id, S.name, S.startdate, S.enddate, S.status, S.questions FROM eb_surveys S WHERE S.id = '" + request.Id + "';";
			}
			EbDataSet dt = this.EbConnectionFactory.DataDB.DoQueries(qryStr, new DbParameter[] { });
			if(dt.Tables.Count > 0)
			{
				foreach(EbDataRow dr in dt.Tables[0].Rows)
				{
					questionList.Add(new Eb_SurveyQuestion { Id = Convert.ToInt32(dr[0]), Question = dr[1].ToString(), ChoiceType = Convert.ToInt32(dr[2]) });
				}
			}
			if(dt.Tables.Count > 1 && dt.Tables[1].Rows.Count > 0)
			{
				surveyObj.Id = Convert.ToInt32(dt.Tables[1].Rows[0][0]);
				surveyObj.Name = dt.Tables[1].Rows[0][1].ToString();
				surveyObj.Start = Convert.ToDateTime(dt.Tables[1].Rows[0][2]).ToString("dd-MM-yyyy HH:mm");
				surveyObj.End = Convert.ToDateTime(dt.Tables[1].Rows[0][3]).ToString("dd-MM-yyyy HH:mm");
				surveyObj.Status = Convert.ToInt32(dt.Tables[1].Rows[0][4]);
				surveyObj.QuesIds = dt.Tables[1].Rows[0][5].ToString().Split(",").Select(Int32.Parse).ToList(); 
			}


			return new ManageSurveyResponse() {Obj = surveyObj, AllQuestions = questionList };
		}

		public GetParticularSurveyResponse Post(GetParticularSurveyRequest request)
		{
			string qryStr = @"	SELECT name FROM eb_surveys WHERE id=:id;
							SELECT * FROM
								(SELECT UNNEST(string_to_array(S.questions, ',')::int[]) AS q_id FROM eb_surveys S WHERE id=:id) QUES_IDS, 
								(SELECT Q.id, Q.query, Q.q_type, C.choice FROM eb_survey_queries Q, eb_query_choices C
									WHERE Q.id = C.q_id AND C.eb_del='F') QUES_ANS
								WHERE QUES_IDS.q_id=QUES_ANS.id;";

			EbDataSet dt = this.EbConnectionFactory.DataDB.DoQueries(qryStr, new DbParameter[] { this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.SurveyId) });
			Dictionary<int, Eb_SurveyQuestion> _queries = new Dictionary<int, Eb_SurveyQuestion>();
			string _surveyname = string.Empty;

			if(dt.Tables.Count > 1)
			{
				_surveyname = dt.Tables[0].Rows[0][0].ToString();
				int _serialno = 1, _oldqid = 0, _newqid;
				Eb_SurveyQuestion _question = null;
				foreach (EbDataRow dr in dt.Tables[1].Rows)
				{
					_newqid = Convert.ToInt32(dr[1]);
					if (_oldqid != _newqid)
					{
						_question = new Eb_SurveyQuestion() { Id = _newqid, Question = dr[2].ToString(), ChoiceType = Convert.ToInt32(dr[3]), Choices = new List<string> { dr[4].ToString() } };
						_queries.Add(_serialno++, _question);
						_oldqid = _newqid;
					}
					else
					{
						_question.Choices.Add(dr[4].ToString());
					}					
				}
			}
			return new GetParticularSurveyResponse {SurveyId =  request.SurveyId, SurveyName = _surveyname, Queries = _queries};
		}


		

		public SaveSurveyResponse Post(SaveSurveyRequest request)
		{
			var rstatus = 0;
			Eb_Survey surveyObj = JsonConvert.DeserializeObject<Eb_Survey>(request.Data);
			List<DbParameter> parameters = new List<DbParameter>();
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, surveyObj.Id));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("name", EbDbTypes.String, surveyObj.Name));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("start", EbDbTypes.DateTime, DateTime.ParseExact(surveyObj.Start, "dd-MM-yyyy HH:mm", null)));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("end", EbDbTypes.DateTime, DateTime.ParseExact(surveyObj.End, "dd-MM-yyyy HH:mm", null)));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("status", EbDbTypes.Int32, surveyObj.Status));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("questions", EbDbTypes.String, surveyObj.QuesIds.Select(i => i.ToString(CultureInfo.InvariantCulture)).Aggregate((s1, s2) => s1 + "," + s2)));
			
			if (surveyObj.Id > 0)
			{
				string qryStr = @"UPDATE eb_surveys SET name=:name, startdate=:start, enddate=:end, status=:status, questions=:questions WHERE id=:id;";
				rstatus = this.EbConnectionFactory.ObjectsDB.UpdateTable(qryStr, parameters.ToArray());
			}
			else
			{
				string qryStr = @"INSERT INTO eb_surveys(name, startdate, enddate, status, questions) VALUES (:name, :start, :end, :status, :questions) RETURNING id;";
				EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery(qryStr, parameters.ToArray());
				rstatus = Convert.ToInt32(dt.Rows[0][0]);
			}			
			return new SaveSurveyResponse() { Status = rstatus};
		}

		public GetSurveyListResponse Post(GetSurveyListRequest request)
		{
			string qryStr = @"SELECT id, name FROM eb_surveys;";
			EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(qryStr, new DbParameter[] { });
			Dictionary<int, string> dict = new Dictionary<int, string>();
			foreach(EbDataRow dr in dt.Rows)
			{
				dict.Add(Convert.ToInt32(dr[0]), dr[1].ToString());
			}
			return (new GetSurveyListResponse { SurveyDict = dict });
		}

        public GetSurveysByAppResponse Get(GetSurveysByAppRequest request)
        {
            List<Eb_Survey> list = new List<Eb_Survey>();
            string query = "SELECT id,name,startdate,enddate,status,questions FROM eb_surveys";
            string quesid = string.Empty;
            try
            {
                var dt = this.EbConnectionFactory.ObjectsDB.DoQuery(query);
                foreach (EbDataRow row in dt.Rows)
                {
                    quesid = row[5].ToString();

                    list.Add(new Eb_Survey
                    {
                        Id = Convert.ToInt32(row[0]),
                        Name = row[1] as string,
                        Start = Convert.ToDateTime(row[2]).ToString("dd-MM-yyyy"),
                        End = Convert.ToDateTime(row[3]).ToString("dd-MM-yyyy"),
                        Status = Convert.ToInt32(row[4]),
                        QuesIds = quesid.Split(",").Select(Int32.Parse).ToList<int>()
                    });
                }
            }
            catch(Exception ee)
            {
                Console.WriteLine(ee.Message);
            }
            
            return new GetSurveysByAppResponse { Surveys = list};
        }
	}
}

