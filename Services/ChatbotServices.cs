using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.Objects.DVRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Globalization;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack
{
    [ClientCanSwapTemplates]
    [Authenticate]
    public class ChatbotServices : EbBaseService
    {
        public ChatbotServices(IEbConnectionFactory _dbf) : base(_dbf) { }


        public GetAppListResponse Get(AppListRequest request)
        {
            string Query1 = @"
                            SELECT
                                    applicationname, application_type, id
                            FROM    
                                    eb_applications;
                        ";
            var table = this.EbConnectionFactory.ObjectsDB.DoQuery(Query1);
            GetAppListResponse resp = new GetAppListResponse();
            foreach (EbDataRow row in table.Rows)
            {
                string appName = row[0].ToString();
                string appType = row[1].ToString();
                string appId = row[2].ToString();
                if (!resp.AppList.ContainsKey(appType))
                {
                    List<string> list = new List<string>();
                    list.Add(appName);
                    list.Add(appId);
                    resp.AppList.Add(appType, list);

                }
                else
                {
                    resp.AppList[appType].Add(appName);
                    resp.AppList[appType].Add(appId);
                }
            }
            //int _id = Convert.ToInt32(request.BotFormIds);
            //var myService = base.ResolveService<EbObjectService>();
            //var res = (EbObjectFetchLiveVersionResponse)myService.Get(new EbObjectFetchLiveVersionRequest() { Id = _id });
            return resp;
        }


        public GetBotDetailsResponse Get(BotDetailsRequest request)
        {
            var Query1 = @"
SELECT 
	name, 
	url, 
	welcome_msg, 
	fullname, 
	botid,
    id
    
FROM 
	eb_bots 
WHERE 
	app_id = @appid;";
            EbDataTable table = this.EbConnectionFactory.ObjectsDB.DoQuery(Query1.Replace("@appid", request.AppId.ToString()));
            GetBotDetailsResponse resp = new GetBotDetailsResponse();
            foreach (EbDataRow row in table.Rows)
            {
                resp.Name = row[0].ToString();
                resp.Url = row[1].ToString();
                resp.WelcomeMsg = row[2].ToString();
                resp.FullName = row[3].ToString();
                resp.BotId = row[4].ToString();
                resp.Id = Convert.ToInt32(row[5]);
            }
            return resp;
        }


        public GetBotForm4UserResponse Get(GetBotForm4UserRequest request)
        {
            /*var Query1 = @"
                            SELECT DISTINCT
		                            EOV.refid, EO.obj_name 
                            FROM
		                            eb_objects EO, eb_objects_ver EOV, eb_objects_status EOS, eb_objects2application EOTA
                            WHERE 
		                            EO.id = EOV.eb_objects_id  AND
		                            EO.id = EOTA.obj_id  AND
		                            EOS.eb_obj_ver_id = EOV.id AND
		                            EO.id =  ANY(@Ids) AND 
		                            EOS.status = 3 AND
		                            ( 	
			                            EO.obj_type = 16 OR
			                            EO.obj_type = 17
			                            OR EO.obj_type = 18
		                            )  AND
		                            EOTA.app_id = @appid AND
                                    EOTA.eb_del = 'F'
                        ";*/

            EbDataTable table = this.EbConnectionFactory.ObjectsDB.DoQuery(this.EbConnectionFactory.ObjectsDB.EB_GET_BOT_FORM.Replace("@Ids", request.BotFormIds).Replace("@appid", request.AppId));
            GetBotForm4UserResponse resp = new GetBotForm4UserResponse();
            foreach (EbDataRow row in table.Rows)
            {
                string formRefid = row[0].ToString();
                string formName = row[1].ToString();
                resp.BotForms.Add(formRefid, formName);
            }
            //int _id = Convert.ToInt32(request.BotFormIds);
            //var myService = base.ResolveService<EbObjectService>();
            //var res = (EbObjectFetchLiveVersionResponse)myService.Get(new EbObjectFetchLiveVersionRequest() { Id = _id });
            return resp;
        }

        public CreateBotResponse Post(CreateBotRequest request)
        {
            string botid = null;
            try
            {
                using (var con = this.EbConnectionFactory.ObjectsDB.GetNewConnection())
                {
                    con.Open();
                    DbCommand cmd = null;
                    string sql = "SELECT * FROM eb_createbot(@solid, @name, @fullname, @url, @welcome_msg, @uid, @botid)";
                    cmd = this.EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);
                    cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@solid", EbDbTypes.String, request.SolutionId));
                    cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@name", EbDbTypes.String, request.BotName));
                    cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@fullname", EbDbTypes.String, request.FullName));
                    cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@url", EbDbTypes.String, request.WebURL));
                    cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@welcome_msg", EbDbTypes.String, request.WelcomeMsg));
                    cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@uid", EbDbTypes.Int32, request.UserId));
                    cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@botid", EbDbTypes.Int32, (request.BotId != null) ? request.BotId : "0"));

                    botid = cmd.ExecuteScalar().ToString();
                }
            }
            catch (Exception e)
            {

            }
            return new CreateBotResponse() { BotId = botid };
        }

        List<System.Data.Common.DbParameter> parameters = new List<System.Data.Common.DbParameter>();

        [CompressResponse]
        public object Get(BotListRequest request)
        {
            List<ChatBot> res = new List<ChatBot>();
            string sql = @"
SELECT 
    id,
	name, 
	url, 
	botid, 
	(SELECT firstname FROM eb_users WHERE id = eb_bots.created_by) AS created_by, 
	created_at, 
	(SELECT firstname FROM eb_users WHERE id = eb_bots.modified_by) AS modified_by, 
	modified_at, welcome_msg 
FROM 
	eb_bots 
WHERE 
	solution_id = @solid;";
            parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@solid", EbDbTypes.Int32, 100));//request.SolutionId));
            var dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql, parameters.ToArray());

            foreach (var dr in dt.Rows)
            {
                ChatBot bot = new ChatBot
                {
                    BotId = dr[0].ToString(),
                    Name = dr[1].ToString(),
                    WebsiteURL = dr[2].ToString(),
                    ChatId = dr[3].ToString(),
                    CreatedBy = dr[4].ToString(),
                    CreatedAt = Convert.ToDateTime(dr[5]),
                    LastModifiedBy = dr[6].ToString(),
                    LastModifiedAt = Convert.ToDateTime(dr[7]),
                    WelcomeMsg = dr[8].ToString()
                };
                res.Add(bot);
            }
            return new BotListResponse { Data = res };
        }

        public object Any(CreateBotFormTableRequest request)
        {

            //string qry = "SELECT EXISTS (SELECT 1 FROM   information_schema.tables WHERE  table_schema = 'public' AND table_name = @tbl); ";
            //DbParameter[] parameter1 = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("@tbl", EbDbTypes.String, request.BotObj.TableName.ToLower()) };
            DbParameter[] parameter1 = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("tbl", EbDbTypes.String, request.BotObj.TableName.ToLower()) };
            var rslt = this.EbConnectionFactory.ObjectsDB.IsTableExists(this.EbConnectionFactory.ObjectsDB.IS_TABLE_EXIST, parameter1);
            string cols = "";
            var Columns = new DVColumnCollection();
			var LineColumns = new DVColumnCollection();
			var LineTableCreated = false;
			var pos = 0;
            var vDbTypes = this.EbConnectionFactory.ObjectsDB.VendorDbTypes;
            foreach (EbControl control in request.BotObj.Controls)
            {
                DVBaseColumn _col = null;
                if (control is EbNumeric)
                {
                    cols += control.Name + " " + vDbTypes.Decimal.VDbType.ToString() + ",";
                    _col = new DVNumericColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.Int32, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
                }
                else if (control is EbTextBox)
                {
                    cols += control.Name + " " + vDbTypes.String.VDbType.ToString() + ",";
                    _col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.String, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
                }
                else if (control is EbDate)
                {
                    if ((control as EbDate).EbDateType == EbDateType.Date)
                    {
                        cols += control.Name + " " + vDbTypes.Date.VDbType.ToString() + ",";
                        _col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.Date, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
                    }
                    else if ((control as EbDate).EbDateType == EbDateType.DateTime)
                    {
                        cols += control.Name + " " + vDbTypes.DateTime.VDbType.ToString() + ",";
                        _col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.DateTime, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
                    }
                    else if ((control as EbDate).EbDateType == EbDateType.Time)
                    {
                        cols += control.Name + " " + vDbTypes.Time.VDbType.ToString() + ",";
                        _col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.Time, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
                    }
                }
                else if (control is EbInputGeoLocation)
                {
                    cols += control.Name + " " + vDbTypes.String.VDbType.ToString() + ",";
                    _col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.String, bVisible = true, sWidth = "100px", ClassName = "dt-body-right tdheight", RenderAs = StringRenderType.Marker };
                }

				else if(control is EbCardSetParent)
				{

					if ((control as EbCardSetParent).MultiSelect)///////
					{
						cols += control.Name + " " + vDbTypes.String.VDbType.ToString() + ",";
						_col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.String, bVisible = true, sWidth = "100px", ClassName = "dt-body-right tdheight", RenderAs = StringRenderType.Link };

						DbParameter[] lineparameter = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("tbl", EbDbTypes.String, request.BotObj.TableName.ToLower() + "_lines") };
						var linerslt = this.EbConnectionFactory.ObjectsDB.IsTableExists(this.EbConnectionFactory.ObjectsDB.IS_TABLE_EXIST, lineparameter);
						string linecols = "";
						var linepos = 0;

						foreach (EbCardField CardField in (control as EbCardSetParent).CardFields)
						{
							if (!CardField.DoNotPersist)
							{
								DVBaseColumn _linecol = null;
								if (CardField is EbCardNumericField)
								{
									linecols += "," + CardField.Name + " " + vDbTypes.Decimal.VDbType.ToString();
									_linecol = new DVNumericColumn { Data = linepos, Name = CardField.Name, sTitle = CardField.Name, Type = EbDbTypes.Double, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
								}
								else
								{
									linecols += "," + CardField.Name + " " + vDbTypes.String.VDbType.ToString();
									_linecol = new DVStringColumn { Data = linepos, Name = CardField.Name, sTitle = CardField.Name, Type = CardField.EbDbType, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
								}

							LineColumns.Add(_linecol);
							linepos++;
							}
						}

						if (!linerslt)
						{
							string sql2 = "CREATE TABLE @tbl( id SERIAL PRIMARY KEY, formid integer, selectedcardid integer @cols)".Replace("@cols", linecols).Replace("@tbl", request.BotObj.TableName + "_lines");
							this.EbConnectionFactory.ObjectsDB.CreateTable(sql2);
							LineTableCreated = true;
						}
						else
						{
							var ColsColl2 = this.EbConnectionFactory.ObjectsDB.GetColumnSchema(request.BotObj.TableName + "_lines");
							var sql2 = "";
							var name2 = "";
							foreach (DVBaseColumn col in LineColumns)
							{
								var flag2 = false;
								name2 = col.Name.ToLower();
								foreach (EbDataColumn dr in ColsColl2)
								{
									if (name2 == (dr.ColumnName.ToLower()))
									{
										//type check
										if (col.Type == dr.Type)
										{
											flag2 = true;
											break;
										}
										else
										{
											flag2 = true;
											//Errorrrrrrrrr...........
										}
									}
									else
										flag2 = false;
								}
								if (!flag2)
									sql2 += "alter table @tbl Add column " + name2 + " " + vDbTypes.GetVendorDbType(col.Type).ToString() + ";";

							}
							if (sql2 != "")
							{
								sql2 = sql2.Replace("@tbl", request.BotObj.TableName + "_" + control.Name);
								var ret2 = this.EbConnectionFactory.ObjectsDB.UpdateTable(sql2);
							}
						}
					}
					//single card
					else
					{
						foreach (EbCardField CardField in (control as EbCardSetParent).CardFields)
						{
							if (!CardField.DoNotPersist)
							{
								if (CardField is EbCardNumericField)
								{
									cols += CardField.Name + " " + vDbTypes.Decimal.VDbType.ToString() + ",";
									_col = new DVNumericColumn { Data = pos, Name = CardField.Name, sTitle = CardField.Name, Type = EbDbTypes.Double, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
								}
								else
								{
									cols += CardField.Name + " " + vDbTypes.String.VDbType.ToString() + ",";
									_col = new DVStringColumn { Data = pos, Name = CardField.Name, sTitle = CardField.Name, Type = CardField.EbDbType, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
								}
								Columns.Add(_col);
								pos++;
							}
						}

					}
				}
                else
                {
                    cols += control.Name + " " + vDbTypes.String.VDbType.ToString() + ",";
                    _col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.String, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
                }
                Columns.Add(_col);
                pos++;
            }

            if (!rslt)
            {
                var str = "id SERIAL PRIMARY KEY,";
                cols = str + cols;
                str = "eb_created_by " + vDbTypes.Int32.VDbType.ToString() + ",";
                str += "eb_created_at " + vDbTypes.DateTime.VDbType.ToString() + ",";
                str += "eb_lastmodified_by " + vDbTypes.Int32.VDbType.ToString() + ",";
                str += "eb_lastmodified_at " + vDbTypes.DateTime.VDbType.ToString() + ",";
                str += "eb_del " + vDbTypes.Boolean.VDbType.ToString() + " DEFAULT 'F',";
                str += "eb_void " + vDbTypes.Boolean.VDbType.ToString() + " DEFAULT 'F',";
                str += "transaction_date " + vDbTypes.DateTime.VDbType.ToString() + ",";
                str += "autogen " + vDbTypes.Int64.VDbType.ToString();
                cols += str;
                string sql = "CREATE TABLE @tbl(@cols)".Replace("@cols", cols).Replace("@tbl", request.BotObj.TableName);
                this.EbConnectionFactory.ObjectsDB.CreateTable(sql);

				if (LineTableCreated)
				{
					string rfid = CreateDsAndDv4Cards(request, LineColumns);
					foreach(DVBaseColumn col in Columns)
					{
						if((col as DVStringColumn).RenderAs == StringRenderType.Link)
						{
							(col as DVStringColumn).LinkRefId = rfid;
						}
					}
				}
				CreateDsAndDv(request, Columns);

			}

            //Alter Table
            else
            {
                //string sql = @"select column_name,data_type from information_schema.columns
                //                where table_name = '@tbl';".Replace("@tbl", request.BotObj.TableName);
                //EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql);
                var ColsColl = this.EbConnectionFactory.ObjectsDB.GetColumnSchema(request.BotObj.TableName);
                var sql = "";
                var name = "";
                foreach (DVBaseColumn col in Columns)
                {
                    var flag = false;
                    name = col.Name.ToLower();
                    foreach (EbDataColumn dr in ColsColl)
                    {
                        if (name == (dr.ColumnName.ToLower()))
                        {
                            //type check
                            if (col.Type == dr.Type)
                            {
                                flag = true;
                                break;
                            }
                            else
                            {
                                flag = true;
                               //Errorrrrrrrrr...........
                            }
                        }
                        else
                            flag = false;
                    }
                    if (!flag)
                        sql += "alter table @tbl Add column " + name + " " + vDbTypes.GetVendorDbType(col.Type).ToString() + ";";

                }
                if (sql != "")
                {
                    sql = sql.Replace("@tbl", request.BotObj.TableName);
                    var ret = this.EbConnectionFactory.ObjectsDB.UpdateTable(sql);
                }
            }

            return new CreateBotFormTableResponse();
        }

        public void CreateDsAndDv(CreateBotFormTableRequest request, DVColumnCollection Columns)
        {
            var dsobj = new EbDataSource();
            dsobj.Sql = "SELECT * FROM @tbl".Replace("@tbl", request.BotObj.TableName);
            var ds = new EbObject_Create_New_ObjectRequest();
            ds.Name = request.BotObj.Name + "_datasource";
            ds.Description = "desc";
            ds.Json = EbSerializers.Json_Serialize(dsobj);
            ds.Status = ObjectLifeCycleStatus.Live;
            ds.Relations = "";
            ds.IsSave = false;
            ds.Tags = "";
            ds.Apps = request.Apps;
            ds.TenantAccountId = request.TenantAccountId;
            ds.WhichConsole = request.WhichConsole;
            ds.UserId = request.UserId;
            var myService = base.ResolveService<EbObjectService>();
            var res = myService.Post(ds);
            var refid = res.RefId;

            var dvobj = new EbTableVisualization();
            dvobj.DataSourceRefId = refid;
            dvobj.Columns = Columns;
            dvobj.DSColumns = Columns;
            var ds1 = new EbObject_Create_New_ObjectRequest();
            ds1.Name = request.BotObj.Name + "_response";
            ds1.Description = "desc";
            ds1.Json = EbSerializers.Json_Serialize(dvobj);
            ds1.Status = ObjectLifeCycleStatus.Live;
            ds1.Relations = refid;
            ds1.IsSave = false;
            ds1.Tags = "";
            ds1.Apps = request.Apps;
            ds1.TenantAccountId = request.TenantAccountId;
            ds1.WhichConsole = request.WhichConsole;
            ds1.UserId = request.UserId;
            var res1 = myService.Post(ds1);
            var refid1 = res.RefId;
        }

		public string CreateDsAndDv4Cards(CreateBotFormTableRequest request, DVColumnCollection Columns)
		{
			var dsobj = new EbDataSource();
			dsobj.Sql = "SELECT * FROM @tbl WHERE formid = :id".Replace("@tbl", request.BotObj.TableName+"_lines");
			var ds = new EbObject_Create_New_ObjectRequest();
			ds.Name = request.BotObj.Name + "_datasource4Card";
			ds.Description = "desc";
			ds.Json = EbSerializers.Json_Serialize(dsobj);
			ds.Status = ObjectLifeCycleStatus.Live;
			ds.Relations = "";
			ds.IsSave = false;
			ds.Tags = "";
			ds.Apps = request.Apps;
			ds.TenantAccountId = request.TenantAccountId;
			ds.WhichConsole = request.WhichConsole;
			ds.UserId = request.UserId;
			var myService = base.ResolveService<EbObjectService>();
			var res = myService.Post(ds);
			var refid = res.RefId;

			var dvobj = new EbTableVisualization();
			dvobj.DataSourceRefId = refid;
			dvobj.Columns = Columns;
			dvobj.DSColumns = Columns;
			var ds1 = new EbObject_Create_New_ObjectRequest();
			ds1.Name = request.BotObj.Name + "_response4Card";
			ds1.Description = "desc";
			ds1.Json = EbSerializers.Json_Serialize(dvobj);
			ds1.Status = ObjectLifeCycleStatus.Live;
			ds1.Relations = refid;
			ds1.IsSave = false;
			ds1.Tags = "";
			ds1.Apps = request.Apps;
			ds1.TenantAccountId = request.TenantAccountId;
			ds1.WhichConsole = request.WhichConsole;
			ds1.UserId = request.UserId;
			var res1 = myService.Post(ds1);
			var refid1 = res.RefId;

			return refid;
		}

		public object Any(InsertIntoBotFormTableRequest request)
        {
            DbParameter parameter1;
            List<DbParameter> paramlist = new List<DbParameter>();
            string cols = "";
            string vals = "";

			DbParameter Param4Lines;
			string Cols4Lines = "";
			List<string> Vals4Lines = new List<string>(); ;
			int cardCount = 0;

			foreach (var obj in request.Fields)
            {
                cols += obj.Name + ",";
                if (obj.Type == EbDbTypes.Decimal)
                {
                    vals += ":" + obj.Name + ",";
                    parameter1 = this.EbConnectionFactory.ObjectsDB.GetNewParameter(obj.Name, EbDbTypes.Decimal, double.Parse(obj.Value));
                }
                else if (obj.Type == EbDbTypes.Date)
                {
                    vals += ":" + obj.Name + ",";
                    parameter1 = this.EbConnectionFactory.ObjectsDB.GetNewParameter(obj.Name, EbDbTypes.Date, DateTime.Parse(obj.Value));
                }
                else if (obj.Type == EbDbTypes.DateTime)
                {
                    vals += ":" + obj.Name + ",";
                    parameter1 = this.EbConnectionFactory.ObjectsDB.GetNewParameter(obj.Name, EbDbTypes.DateTime, DateTime.Parse(obj.Value));
                }
                else if (obj.Type == EbDbTypes.Time)
                {
                    vals += ":" + obj.Name + ",";
                    DateTime dt = DateTime.Parse(obj.Value);
                    //DateTime.ParseExact(obj.Value, "HH:mm:ss", CultureInfo.InvariantCulture)
                    parameter1 = this.EbConnectionFactory.ObjectsDB.GetNewParameter(obj.Name, EbDbTypes.Time, dt.ToString("HH:mm:ss"));
                }
				else if(obj.Type == EbDbTypes.Json)
				{
					vals += ":" + obj.Name + ",";
					parameter1 = this.EbConnectionFactory.ObjectsDB.GetNewParameter(obj.Name, EbDbTypes.String, "CARD_LINES");

					//Insert to table _lines
					
					Dictionary<int,List<BotInsert>> ObjectLines = JsonConvert.DeserializeObject<Dictionary<int, List<BotInsert>>>(obj.Value);
					
					foreach (KeyValuePair<int, List<BotInsert>> card in ObjectLines)
					{
						// do something with card.Value or card.Key
						string Vals4SingleLine = "";
						foreach (var objLines in card.Value)
						{
							if(cardCount == 0)
								Cols4Lines += objLines.Name + ",";
							if (objLines.Type == EbDbTypes.Double)
							{
								Vals4SingleLine += ":line" + cardCount + objLines.Name + ",";
								Param4Lines = this.EbConnectionFactory.ObjectsDB.GetNewParameter("line"+ cardCount + objLines.Name, EbDbTypes.Double, double.Parse(objLines.Value));
							}
							else
							{
								Vals4SingleLine += ":line"+ cardCount + objLines.Name + ",";
								Param4Lines = this.EbConnectionFactory.ObjectsDB.GetNewParameter("line"+ cardCount + objLines.Name, EbDbTypes.String, objLines.Value);
							}
							paramlist.Add(Param4Lines);
						}
						if (cardCount == 0)
							Cols4Lines += "selectedcardid";
						Vals4SingleLine += ":line" + cardCount + "_selected_card_id";
						paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("line" + cardCount + "_selected_card_id", EbDbTypes.Int32, card.Key));
						Vals4Lines.Add(Vals4SingleLine);
						cardCount++;
					}





				}
                else
                {
                    vals += ":" + obj.Name + ",";
                    parameter1 = this.EbConnectionFactory.ObjectsDB.GetNewParameter(obj.Name, EbDbTypes.String, obj.Value);
                }
                paramlist.Add(parameter1);
            }
            cols = cols+ "eb_created_by,eb_created_at,eb_lastmodified_by,eb_lastmodified_at,transaction_date,autogen";
            vals = vals+ ":eb_created_by,:eb_created_at,:eb_lastmodified_by,:eb_lastmodified_at,:transaction_date,:autogen";
            paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_created_by", EbDbTypes.Int32, request.UserId));
            paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_created_at", EbDbTypes.DateTime, DateTime.Now));
            paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_lastmodified_by", EbDbTypes.Int32, request.UserId));
            paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_lastmodified_at", EbDbTypes.DateTime, DateTime.Now));
            paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("transaction_date", EbDbTypes.DateTime, DateTime.Now));
            paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("autogen", EbDbTypes.Int64, new Random().Next()));
            var qry = string.Format("insert into @tbl({0}) values({1})".Replace("@tbl", request.TableName), cols, vals);

			//append second insert query
			if (!Cols4Lines.IsNullOrEmpty())
			{
				qry = "WITH rows AS (" + qry + " returning id)";
				qry += "INSERT INTO " + request.TableName + "_lines(formid," + Cols4Lines + ") ";

				qry += "(SELECT rows.id," + Vals4Lines[0] + " FROM rows)";
				for(int i = 1; i < Vals4Lines.Count; i++)
				{
					qry += " UNION ALL (SELECT rows.id," + Vals4Lines[i] + " FROM rows)";
				}
			}

			var rslt = this.EbConnectionFactory.ObjectsDB.InsertTable(qry, paramlist.ToArray());
            return new InsertIntoBotFormTableResponse();
        }

    }
}
