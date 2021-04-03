using ExpressBase.Common;
using ExpressBase.Common.Application;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class SurveyServices : EbBaseService
    {
        public SurveyServices(IEbConnectionFactory _dbf) : base(_dbf) { }

        public GetSurveyQueriesResponse Get(GetSurveyQueriesRequest request)
        {
            Dictionary<int, EbSurveyQuery> dict = new Dictionary<int, EbSurveyQuery>();

            string sql = @"SELECT Q.id,Q.query,Q.q_type,C.id,C.choice,C.score from eb_survey_queries Q 
                           INNER JOIN eb_query_choices C ON C.q_id = q.id WHERE C.eb_del = 'F';";

            EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(sql);
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

        public GetSurveyQuestionsResponse Get(GetSurveyQuestionsRequest request)
        {
            List<String> qsns = new List<String>();

            string sql = @"SELECT id,question FROM eb_question_bank WHERE eb_del = 'F';";
            EbDataTable dt;
            try
            {
                dt = this.EbConnectionFactory.DataDB.DoQuery(sql);
                foreach (EbDataRow dr in dt.Rows)
                {
                    qsns.Add(dr[1].ToString());
                }
                return new GetSurveyQuestionsResponse { Data = qsns };
            }
            catch (Exception e)
            {
                ;
            }
            return new GetSurveyQuestionsResponse { };
        }

        public SaveQuestionResponse Post(SaveQuestionRequest request)
        {
            EbQuestion question = request.Query;
            string question_s = EbSerializers.Json_Serialize(request.Query);
            SaveQuestionResponse response = new SaveQuestionResponse();
            StringBuilder s = new StringBuilder();
            List<DbParameter> parameters = new List<DbParameter>();
            if (question.QId > 0)
            {
            }
            else
            {
                s.Append("INSERT INTO eb_question_bank(question, eb_del) VALUES(:question, 'F') returning id");
                parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("question", EbDbTypes.Json, question_s));
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(s.ToString(), parameters.ToArray());
                if (Convert.ToInt32(dt.Rows[0][0]) > 0)
                {
                    response.Status = true;
                    response.Quesid = Convert.ToInt32(dt.Rows[0][0]);
                }
                else
                    response.Status = false;
            }
            return response;
        }
        public SurveyQuesResponse Post(SurveyQuesRequest request)
        {
            EbSurveyQuery ques = request.Query;
            SurveyQuesResponse resp = new SurveyQuesResponse();
            int c_count = 0;
            StringBuilder s = new StringBuilder();
            List<DbParameter> parameters = new List<DbParameter>();
            if (ques.QuesId > 0)
            {
                s.Append("UPDATE eb_survey_queries SET query=:query,q_type=:qtype WHERE id=:qid;");
                parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("query", EbDbTypes.String, ques.Question));
                parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("qtype", EbDbTypes.Int32, ques.QuesType));
                parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("qid", EbDbTypes.Int32, ques.QuesId));

                foreach (QueryChoices choice in ques.Choices)
                {
                    int count = c_count++;

                    if (choice.EbDel)
                    {
                        s.Append("UPDATE eb_query_choices SET eb_del = 'T' WHERE id=:choiceid" + count + ";");
                        parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("choiceid" + count, EbDbTypes.Int32, choice.ChoiceId));
                    }
                    else if (choice.IsNew)
                    {
                        s.Append("INSERT INTO eb_query_choices(q_id,choice,score) VALUES(:questid" + count + ",:choice" + count + ",:chscore" + count + ");");
                        parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("questid" + count, EbDbTypes.Int32, ques.QuesId));
                        parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("choice" + count, EbDbTypes.String, choice.Choice));
                        parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("chscore" + count, EbDbTypes.Int32, choice.Score));
                    }
                    else
                    {
                        s.Append("UPDATE eb_query_choices SET choice=:choice" + count + ",score=:chscore" + count + " WHERE id=:chid" + count + ";");
                        parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("choice" + count, EbDbTypes.String, choice.Choice));
                        parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("chid" + count, EbDbTypes.Int32, choice.ChoiceId));
                        parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("chscore" + count, EbDbTypes.Int32, choice.Score));
                    }
                }
                var res = this.EbConnectionFactory.DataDB.DoNonQuery(s.ToString(), parameters.ToArray());
                if (res > 0)
                {
                    resp.Status = true;
                }
            }
            else
            {
                s.Append("INSERT INTO eb_survey_queries(query,q_type) VALUES(:query,:qtype);");
                parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("query", EbDbTypes.String, ques.Question));
                parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("qtype", EbDbTypes.Int32, ques.QuesType));

                s.Append("INSERT INTO eb_query_choices(q_id,choice,score) VALUES");

                foreach (QueryChoices choice in ques.Choices)
                {
                    int count = c_count++;
                    if (EbConnectionFactory.DataDB.Vendor == DatabaseVendors.MYSQL)
                    {
                        s.Append("(last_insert_id(),:choice" + count + ",:score" + count + ")");
                    }
                    else
                    {
                        s.Append("(currval('eb_survey_queries_id_seq'),:choice" + count + ",:score" + count + ")");
                    }
                    if (choice != ques.Choices.Last())
                        s.Append(",");
                    parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("choice" + count, EbDbTypes.String, choice.Choice));
                    parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("score" + count, EbDbTypes.Int32, choice.Score));
                }
                if (EbConnectionFactory.DataDB.Vendor == DatabaseVendors.MYSQL)
                    s.Append(";SELECT last_insert_id();");
                else
                    s.Append(";SELECT currval('eb_survey_queries_id_seq');");

                EbDataSet res = this.EbConnectionFactory.DataDB.DoQueries(s.ToString(), parameters.ToArray());

                if (Convert.ToInt32(res.Tables[0].Rows[0][0]) > 0)
                {
                    resp.Status = true;
                    resp.Quesid = Convert.ToInt32(res.Tables[0].Rows[0][0]);
                }
                else
                    resp.Status = false;
            }
            return resp;
        }

        public ManageSurveyResponse Post(ManageSurveyRequest request)
        {
            Eb_Survey surveyObj = new Eb_Survey() { Id = 0 };
            List<EbSurveyQuery> questionList = new List<EbSurveyQuery>();
            string qryStr = @"SELECT Q.id, Q.query, Q.q_type FROM eb_survey_queries Q;";

            if (request.Id > 0)
            {
                qryStr += @"SELECT S.id, S.name, S.startdate, S.enddate, S.status, S.questions FROM eb_surveys S WHERE S.id = '" + request.Id + "';";
            }
            EbDataSet dt = this.EbConnectionFactory.DataDB.DoQueries(qryStr, new DbParameter[] { });
            if (dt.Tables.Count > 0)
            {
                foreach (EbDataRow dr in dt.Tables[0].Rows)
                {
                    questionList.Add(new EbSurveyQuery { QuesId = Convert.ToInt32(dr[0]), Question = dr[1].ToString(), QuesType = Convert.ToInt32(dr[2]) });
                }
            }
            if (dt.Tables.Count > 1 && dt.Tables[1].Rows.Count > 0)
            {
                surveyObj.Id = Convert.ToInt32(dt.Tables[1].Rows[0][0]);
                surveyObj.Name = dt.Tables[1].Rows[0][1].ToString();
                surveyObj.Start = Convert.ToDateTime(dt.Tables[1].Rows[0][2]).ToString("dd-MM-yyyy HH:mm");
                surveyObj.End = Convert.ToDateTime(dt.Tables[1].Rows[0][3]).ToString("dd-MM-yyyy HH:mm");
                surveyObj.Status = Convert.ToInt32(dt.Tables[1].Rows[0][4]);
                surveyObj.QuesIds = dt.Tables[1].Rows[0][5].ToString().Split(",").Select(Int32.Parse).ToList();
            }

            return new ManageSurveyResponse() { Obj = surveyObj, AllQuestions = questionList };
        }

        public GetParticularSurveyResponse Post(GetParticularSurveyRequest request)
        {
            string qryStr = EbConnectionFactory.DataDB.EB_GETPARTICULARSSURVEY;

            EbDataSet dt = this.EbConnectionFactory.DataDB.DoQueries(qryStr, new DbParameter[] {
                this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.SurveyId)
            });

            Dictionary<int, EbSurveyQuery> _queries = new Dictionary<int, EbSurveyQuery>();
            Eb_Survey _surveyobj = null;

            if (dt.Tables.Count > 1 && dt.Tables[0].Rows.Count > 0)
            {
                _surveyobj = new Eb_Survey
                {
                    Name = dt.Tables[0].Rows[0][0].ToString(),
                    Start = Convert.ToDateTime(dt.Tables[0].Rows[0][1]).ToString("dd-MM-yyyy"),
                    End = Convert.ToDateTime(dt.Tables[0].Rows[0][2]).ToString("dd-MM-yyyy"),
                    Status = Convert.ToInt32(dt.Tables[0].Rows[0][3])
                };

                foreach (EbDataRow dr in dt.Tables[1].Rows)
                {
                    if (!_queries.ContainsKey(Convert.ToInt32(dr[0])))
                    {
                        _queries.Add(Convert.ToInt32(dr[0]), new EbSurveyQuery
                        {
                            QuesId = Convert.ToInt32(dr[1]),
                            Question = dr[2] as string,
                            QuesType = Convert.ToInt32(dr[3]),
                            Choices = new List<QueryChoices>()
                        });

                        _queries[Convert.ToInt32(dr[0])].Choices.Add(new QueryChoices
                        {
                            ChoiceId = Convert.ToInt32(dr[6]),
                            Choice = dr[4].ToString(),
                            Score = Convert.ToInt32(dr[5])
                        });
                    }
                    else
                    {
                        _queries[Convert.ToInt32(dr[0])].Choices.Add(new QueryChoices
                        {
                            ChoiceId = Convert.ToInt32(dr[6]),
                            Choice = dr[4].ToString(),
                            Score = Convert.ToInt32(dr[5])
                        });
                    }
                }
                return new GetParticularSurveyResponse { SurveyInfo = _surveyobj, Queries = _queries };
            }
            else
                return new GetParticularSurveyResponse { };

        }

        public SaveSurveyResponse Post(SaveSurveyRequest request)
        {
            int rstatus = 0;
            Eb_Survey surveyObj = JsonConvert.DeserializeObject<Eb_Survey>(request.Data);
            List<DbParameter> parameters = new List<DbParameter>();
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, surveyObj.Id));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("name", EbDbTypes.String, surveyObj.Name));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("start", EbDbTypes.DateTime, DateTime.ParseExact(surveyObj.Start, "dd-MM-yyyy HH:mm", null)));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("end", EbDbTypes.DateTime, DateTime.ParseExact(surveyObj.End, "dd-MM-yyyy HH:mm", null)));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("status", EbDbTypes.Int32, surveyObj.Status));
            parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("questions", EbDbTypes.String, surveyObj.QuesIds.Select(i => i.ToString(CultureInfo.InvariantCulture)).Aggregate((s1, s2) => s1 + "," + s2)));

            if (surveyObj.Id > 0)
            {
                string qryStr = @"UPDATE eb_surveys SET name=:name, startdate=:start, enddate=:end, status=:status, questions=:questions WHERE id=:id;";
                rstatus = this.EbConnectionFactory.DataDB.UpdateTable(qryStr, parameters.ToArray());
            }
            else
            {
                string qryStr = EbConnectionFactory.DataDB.EB_SAVESURVEY;
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(qryStr, parameters.ToArray());
                rstatus = Convert.ToInt32(dt.Rows[0][0]);
            }
            return new SaveSurveyResponse() { Status = rstatus };
        }

        public GetSurveyListResponse Post(GetSurveyListRequest request)
        {
            string qryStr = @"SELECT id, name FROM eb_surveys;";
            EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(qryStr, new DbParameter[] { });
            Dictionary<int, string> dict = new Dictionary<int, string>();
            foreach (EbDataRow dr in dt.Rows)
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
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query);
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
            catch (Exception ee)
            {
                Console.WriteLine(ee.Message);
            }

            return new GetSurveysByAppResponse { Surveys = list };
        }

        public SurveyMasterResponse Post(SurveyMasterRequest request)
        {
            int id;
            string q = "SELECT id FROM eb_survey_master WHERE userid=:userid AND anonid=:aid AND surveyid=:surid;";

            EbDataTable mid = this.EbConnectionFactory.DataDB.DoQuery(q, new DbParameter[] {
                this.EbConnectionFactory.DataDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
                this.EbConnectionFactory.DataDB.GetNewParameter("aid", EbDbTypes.Int32, request.AnonId),
                this.EbConnectionFactory.DataDB.GetNewParameter("surid", EbDbTypes.Int32, request.SurveyId)
            });
            if (mid.Rows.Count <= 0)
            {
                string q1 = EbConnectionFactory.DataDB.EB_SURVEYMASTER;

                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(q1, new DbParameter[] {
                    this.EbConnectionFactory.DataDB.GetNewParameter("sid", EbDbTypes.Int32, request.SurveyId),
                    this.EbConnectionFactory.DataDB.GetNewParameter("uid", EbDbTypes.Int32, request.UserId),
                    this.EbConnectionFactory.DataDB.GetNewParameter("anid", EbDbTypes.Int32, request.AnonId)
                    });
                id = Convert.ToInt32(dt.Rows[0][0]);
            }
            else
                id = Convert.ToInt32(mid.Rows[0][0]);

            return new SurveyMasterResponse { Id = id };
        }

        public SurveyLinesResponse Post(SurveyLinesRequest request)
        {
            var q = "INSERT INTO eb_survey_lines(masterid,questionid,eb_createdate,choiceids,questype,answer) VALUES(:msid,:qsid,now(),:chid,:qtype,:aswr)";
            string answer = string.Empty;
            if (request.QuesType == 1 || request.QuesType == 2)
                answer = "Not Applicable";
            else
                answer = request.Answer;
            var dt = this.EbConnectionFactory.DataDB.DoNonQuery(q, new DbParameter[] {
                    this.EbConnectionFactory.DataDB.GetNewParameter("msid", EbDbTypes.Int32, request.MasterId),
                    this.EbConnectionFactory.DataDB.GetNewParameter("qsid", EbDbTypes.Int32, request.QuesId),
                    this.EbConnectionFactory.DataDB.GetNewParameter("chid", EbDbTypes.String, request.ChoiceIds),
                    this.EbConnectionFactory.DataDB.GetNewParameter("qtype", EbDbTypes.Int32, request.QuesType),
                    this.EbConnectionFactory.DataDB.GetNewParameter("aswr", EbDbTypes.String, answer)
                    });

            return new SurveyLinesResponse { };
        }

        public GetSurveyEnqResponse Get(GetSurveyEnqRequest request)
        {
            return new GetSurveyEnqResponse { };
        }

        public GetRenderQuestionResponse Get(GetRenderQuestionsRequest Request)
        {

            GetRenderQuestionResponse Resp = new GetRenderQuestionResponse();
            string qry_ = $@"select * from (
select id,form_refid , form_data_id, control_id,ques_id,ext_props from eb_ques_config) A
left join
(select id, question from eb_question_bank) B 
ON B.id = A.ques_id AND B.id is not null";
            EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(qry_);
            for (int i = 0; i < ds.Tables[0].Rows.Count; i++)
            {
                Resp.GetRenderQuestions.Add(
                    new GetRenderQuestions()
                    {
                        id = Convert.ToInt32(ds.Tables[0].Rows[i]["id"]),
                        FormDataId = Convert.ToString(ds.Tables[0].Rows[i]["form_data_id"]),
                        FormRefid = Convert.ToString(ds.Tables[0].Rows[i]["form_refid"]),
                        ControlId = Convert.ToString(ds.Tables[0].Rows[i]["control_id"]),
                        ExtProps = Convert.ToString(ds.Tables[0].Rows[i]["ext_props"]),
                        QuestionID = Convert.ToInt32(ds.Tables[0].Rows[i]["ques_id"]),
                        Questions = Convert.ToString(ds.Tables[0].Rows[i]["question"]),
                    });
            }
            return Resp;
        }
    }
}
