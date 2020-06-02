using ExpressBase.Common;
using ExpressBase.Common.Application;
using ExpressBase.Common.Data;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using ServiceStack;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace ExpressBase.ServiceStack
{
	[ClientCanSwapTemplates]
	[Authenticate]
	public class ChatbotServices : EbBaseService
	{
		public ChatbotServices(IEbConnectionFactory _dbf) : base(_dbf) { }

		public CreateBotFormTableResponse Any(CreateBotFormTableRequest request)// redirecting to Webform table create logic
		{
			EbBotForm _botForm = string.IsNullOrEmpty(request.BotObj.RefId) ? null : this.Redis.Get<EbBotForm>(request.BotObj.RefId);

			if (string.IsNullOrEmpty(request.BotObj.WebFormRefId) && string.IsNullOrEmpty(_botForm?.WebFormRefId))// new object
			{
				EbWebForm WebForm = new EbWebForm();
				this.CompareBothForms(request.BotObj, WebForm);
				string _rel_obj_tmp = string.Join(",", WebForm.DiscoverRelatedRefids());

				EbObject_Create_New_ObjectRequest _form_req = new EbObject_Create_New_ObjectRequest
				{
					Name = WebForm.Name,
					Description = WebForm.Description ?? string.Empty,
					Json = EbSerializers.Json_Serialize(WebForm),
					Status = ObjectLifeCycleStatus.Live,
					IsSave = false,
					Tags = string.Empty,
					Apps = request.Apps,
					SolnId = request.SolnId,
					WhichConsole = request.WhichConsole,
					UserId = request.UserId,
					SourceObjId = "0",
					SourceVerID = "0",
					DisplayName = WebForm.DisplayName,
					SourceSolutionId = request.SolnId,
					Relations = _rel_obj_tmp
				};
				EbObjectService myService = base.ResolveService<EbObjectService>();
				EbObject_Create_New_ObjectResponse resp = myService.Post(_form_req);
				if (string.IsNullOrEmpty(resp.RefId))
					throw new FormException(resp.Message);
				request.BotObj.WebFormRefId = resp.RefId;
				SaveObjectRequest(request.BotObj, request.Apps);
			}
			else
			{
				EbWebForm WebForm = this.GetWebFormObject(request.BotObj.WebFormRefId);
				if (this.CompareBothForms(request.BotObj, WebForm))
				{
					SaveObjectRequest(WebForm, request.Apps);
				}
			}
			return new CreateBotFormTableResponse();
		}

		private void SaveObjectRequest(EbObject obj, string Apps)
		{
			string _rel_obj_tmp = string.Join(",", obj.DiscoverRelatedRefids());
			EbObject_SaveRequest req = new EbObject_SaveRequest
			{
				RefId = obj.RefId,
				Name = obj.Name,
				Description = obj.Description,
				Json = EbSerializers.Json_Serialize(obj),
				Relations = _rel_obj_tmp,
				Tags = "",
				Apps = Apps,
				DisplayName = obj.DisplayName
			};
			EbObjectService myService = base.ResolveService<EbObjectService>();
			EbObject_SaveResponse resp = myService.Post(req);
			if (!string.IsNullOrEmpty(resp.Message))
				throw new FormException(resp.Message);
		}

		private bool CompareBothForms(EbBotForm BotForm, EbWebForm WebForm)
		{
			bool changeFlag = false;
			if (WebForm == null)
			{
				WebForm.EbSid = "webform_autogen_1";
				WebForm.Name = BotForm.Name + "_autogen_webform";
				WebForm.DisplayName = BotForm.DisplayName + " AutoGen Webform";
				WebForm.TableName = BotForm.TableName;
				WebForm.Padding = new UISides { Top = 8, Right = 8, Bottom = 8, Left = 8 };
				WebForm.Validators = new List<EbValidator>();
				WebForm.Controls = this.GetMappedControls(BotForm.Controls);
				WebForm.BeforeSave(this);
				changeFlag = true;
			}
			else
			{
				//foreach (EbControl _control in BotForm.Controls)
				//{
				//    if (WebForm.Controls.Find(e => e.Name == _control.Name) == null)
				//    {
				//        changeFlag = true;
				//        break;
				//    }
				//}
				//if (changeFlag == false && WebForm.Name != (BotForm.Name + "_autogen_webform"))
				//    changeFlag = true;
				changeFlag = true;//temp

				WebForm.EbSid = "webform_autogen_1";
				WebForm.Name = BotForm.Name + "_autogen_webform";
				WebForm.DisplayName = BotForm.DisplayName + " AutoGen Webform";
				WebForm.TableName = BotForm.TableName;
				WebForm.Controls = this.GetMappedControls(BotForm.Controls);
				WebForm.BeforeSave(this);
			}
			return changeFlag;
		}

		private List<EbControl> GetMappedControls(List<EbControl> BotControls)
		{
			List<EbControl> WebControls = new List<EbControl>();
			foreach (EbControl _control in BotControls)
			{
				if (_control is EbCardSetParent)
				{
					EbCardSetParent _ctrl = _control as EbCardSetParent;
					List<EbControl> gridCtrls = new List<EbControl>();

					//Card id - power select column is more suitable for dynamic cards
					gridCtrls.Add(new EbDGNumericColumn()
					{
						Name = "card_id",
						DisplayName = "Card Id",
						EbSid = "CardId1",
						DecimalPlaces = 2,
						Title = "Card Id"
					});

					foreach (EbCardField cardField in _ctrl.CardFields)
					{
						if (cardField.DoNotPersist)
							continue;
						if (cardField is EbCardNumericField)
						{
							gridCtrls.Add(new EbDGNumericColumn()
							{
								Name = cardField.Name,
								DisplayName = cardField.DisplayName,
								EbSid = cardField.EbSid,
								DecimalPlaces = 0,
								Title = cardField.Name
							});
						}
						else
						{
							gridCtrls.Add(new EbDGStringColumn()
							{
								Name = cardField.Name,
								DisplayName = cardField.DisplayName,
								EbSid = cardField.EbSid,
								RowsVisible = 3,
								Title = cardField.Name
							});
						}
					}
					EbDataGrid dataGrid = new EbDataGrid()
					{
						Name = _ctrl.Name,
						DisplayName = _ctrl.DisplayName,
						EbSid = _ctrl.EbSid,
						TableName = _ctrl.TableName,
						Controls = gridCtrls,
						Height = 200,
						LeftFixedColumnCount = 1,
						IsShowSerialNumber = true,
						IsColumnsResizable = true,
						IsAddable = true,
						Padding = new UISides() { Top = 8, Right = 8, Bottom = 8, Left = 8 },
						Margin = new UISides() { Top = 4, Right = 4, Bottom = 4, Left = 4 }
					};
					WebControls.Add(dataGrid);
				}
				else
					WebControls.Add(_control);
			}
			return WebControls;
		}

		private EbWebForm GetWebFormObject(string RefId)
		{
			EbWebForm _form = this.Redis.Get<EbWebForm>(RefId);
			if (_form == null)
			{
				EbObjectService myService = base.ResolveService<EbObjectService>();
				EbObjectParticularVersionResponse formObj = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = RefId });
				_form = EbSerializers.Json_Deserialize(formObj.Data[0].Json);
				this.Redis.Set<EbWebForm>(RefId, _form);
			}
			_form.AfterRedisGet(this);
			return _form;
		}

		public GetAppListResponse Get(AppListRequest request)
		{
			string Query1 = @"SELECT
                                    applicationname, application_type, id
                            FROM    
                                    eb_applications;";
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
			string Query1 = @"SELECT name,	url, welcome_msg, fullname,	botid, id    
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
				string formDisplayName = row[2].ToString();
				resp.BotForms.Add(formRefid, formName);
				resp.BotFormsDisp.Add(formRefid, formDisplayName);
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
					string sql = EbConnectionFactory.ObjectsDB.EB_CREATEBOT;
					cmd = this.EbConnectionFactory.ObjectsDB.GetNewCommand(con, sql);
					cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("solid", EbDbTypes.String, request.SolutionId));
					cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("name", EbDbTypes.String, request.BotName));
					cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("fullname", EbDbTypes.String, request.FullName));
					cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("url", EbDbTypes.String, request.WebURL));
					cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("welcome_msg", EbDbTypes.String, request.WelcomeMsg));
					cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("uid", EbDbTypes.Int32, request.UserId));
					cmd.Parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("botid", EbDbTypes.Int32, (request.BotId != null) ? request.BotId : "0"));

					botid = cmd.ExecuteScalar().ToString();
				}
			}
			catch (Exception e)
			{
				Console.WriteLine("Exception: " + e.ToString());
			}
			return new CreateBotResponse() { BotId = botid };
		}

		List<System.Data.Common.DbParameter> parameters = new List<System.Data.Common.DbParameter>();

		[CompressResponse]
		public object Get(BotListRequest request)
		{
			List<ChatBot> res = new List<ChatBot>();
			string sql = @"SELECT id, name,	url, botid, 
	                        (SELECT firstname FROM eb_users WHERE id = eb_bots.created_by) AS created_by, 
	                        created_at, 
	                        (SELECT firstname FROM eb_users WHERE id = eb_bots.modified_by) AS modified_by, 
	                        modified_at, welcome_msg 
                        FROM 
	                        eb_bots 
                        WHERE 
                            solution_id = @solid;";
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("solid", EbDbTypes.Int32, 100));//request.SolutionId));
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

		private void addControlToColl(EbControl control, ref List<TableColumnMeta> _listNamesAndTypes, IVendorDbTypes vDbTypes)
		{
			if (!(control is EbControlContainer))
			{
				if (control is EbNumeric)
					_listNamesAndTypes.Add(new TableColumnMeta { Name = control.Name, Type = vDbTypes.Decimal });
				else if (control is EbDate)
				{
					if ((control as EbDate).EbDateType == EbDateType.Date)
						_listNamesAndTypes.Add(new TableColumnMeta { Name = control.Name, Type = vDbTypes.Date });
					else if ((control as EbDate).EbDateType == EbDateType.DateTime)
						_listNamesAndTypes.Add(new TableColumnMeta { Name = control.Name, Type = vDbTypes.DateTime });
					else if ((control as EbDate).EbDateType == EbDateType.Time)
						_listNamesAndTypes.Add(new TableColumnMeta { Name = control.Name, Type = vDbTypes.Time });
				}
				else //(control is EbTextBox || control is EbInputGeoLocation)
					_listNamesAndTypes.Add(new TableColumnMeta { Name = control.Name, Type = vDbTypes.String });
			}
		}

		//private CreateWebFormTableResponse CreateWebFormTableHelper(CreateWebFormTableRequest request)
		//{
		//    IVendorDbTypes vDbTypes = this.EbConnectionFactory.ObjectsDB.VendorDbTypes;
		//    List<TableColumnMeta> _listNamesAndTypes = new List<TableColumnMeta>();

		//    IEnumerable<EbControl> _flatControls = request.WebObj.Controls.Get1stLvlControls();

		//    foreach (EbControl control in _flatControls)
		//    {
		//        //this.addControlToColl(control, ref _listNamesAndTypes, vDbTypes);
		//        _listNamesAndTypes.Add(new TableColumnMeta { Name = control.Name, Type = control.GetvDbType(vDbTypes) });
		//    }

		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_by", Type = vDbTypes.Decimal });
		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_aid", Type = vDbTypes.Decimal });
		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_at", Type = vDbTypes.DateTime });
		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_by", Type = vDbTypes.Decimal });
		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_aid", Type = vDbTypes.Decimal });
		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_at", Type = vDbTypes.DateTime });
		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_del", Type = vDbTypes.Boolean, Default = "F" });
		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_void", Type = vDbTypes.Boolean, Default = "F" });

		//    CreateOrAlterTable(request.WebObj.TableName.ToLower(), _listNamesAndTypes);

		//    return new CreateWebFormTableResponse();
		//}

		//private CreateWebFormTableResponse CreateWebFormTableRec(CreateWebFormTableRequest request)
		//{
		//    foreach (EbControl _control in request.WebObj.Controls)
		//    {
		//        if (_control is EbControlContainer)
		//        {
		//            EbControlContainer Container = _control as EbControlContainer;
		//            Container.TableName = Container.TableName.IsNullOrEmpty() ? request.WebObj.TableName : Container.TableName;
		//            request.WebObj = Container;
		//            CreateWebFormTableResponse Response = CreateWebFormTableHelper(request);
		//            CreateWebFormTableRec(request);
		//        }
		//    }

		//    return new CreateWebFormTableResponse();
		//}

		//public object Any(CreateBotFormTableRequest request)
		//{
		//    var vDbTypes = this.EbConnectionFactory.DataDB.VendorDbTypes;
		//    List<TableColumnMeta> _listNamesAndTypes = new List<TableColumnMeta>();

		//    foreach (EbControl control in request.BotObj.Controls)
		//    {
		//        if (control is EbNumeric)
		//            _listNamesAndTypes.Add(new TableColumnMeta { Name = control.Name, Type = vDbTypes.Decimal });
		//        else if (control is EbDate)
		//        {
		//            VendorDbType _vdbtype = vDbTypes.String;
		//            if ((control as EbDate).EbDateType == EbDateType.Date)
		//                _vdbtype = vDbTypes.Date;
		//            else if ((control as EbDate).EbDateType == EbDateType.DateTime)
		//                _vdbtype = vDbTypes.DateTime;
		//            else if ((control as EbDate).EbDateType == EbDateType.Time)
		//                _vdbtype = vDbTypes.Time;
		//            _listNamesAndTypes.Add(new TableColumnMeta { Name = control.Name, Type = _vdbtype });
		//        }
		//        else if (control is EbCardSetParent)
		//        {
		//            List<TableColumnMeta> listLine = new List<TableColumnMeta>();
		//            foreach (EbCardField CardField in (control as EbCardSetParent).CardFields)
		//            {
		//                if (!CardField.DoNotPersist)
		//                {
		//                    if (CardField is EbCardNumericField)
		//                        listLine.Add(new TableColumnMeta { Name = CardField.Name, Type = vDbTypes.Decimal });
		//                    else
		//                        listLine.Add(new TableColumnMeta { Name = CardField.Name, Type = vDbTypes.String });
		//                }
		//            }
		//            listLine.Add(new TableColumnMeta { Name = "formid", Type = vDbTypes.Decimal });
		//            listLine.Add(new TableColumnMeta { Name = "itemid", Type = vDbTypes.Decimal });//selected card id

		//            CreateOrAlterTable((request.BotObj.TableName.ToLower() + "_lines"), listLine);
		//        }
		//        else if (control is EbSurvey)
		//        {
		//            _listNamesAndTypes.Add(new TableColumnMeta { Name = control.Name, Type = vDbTypes.String });
		//            List<TableColumnMeta> listLine = new List<TableColumnMeta>();
		//            listLine.Add(new TableColumnMeta { Name = "formid", Type = vDbTypes.Decimal });
		//            listLine.Add(new TableColumnMeta { Name = "itemid", Type = vDbTypes.Decimal });//survey id
		//            listLine.Add(new TableColumnMeta { Name = "surveyid", Type = vDbTypes.Decimal });
		//            listLine.Add(new TableColumnMeta { Name = "option", Type = vDbTypes.String });
		//            CreateOrAlterTable((request.BotObj.TableName.ToLower() + "_lines"), listLine);
		//        }
		//        else //(control is EbTextBox || control is EbInputGeoLocation)
		//            _listNamesAndTypes.Add(new TableColumnMeta { Name = control.Name, Type = vDbTypes.String });
		//    }
		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_by", Type = vDbTypes.Decimal });
		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_aid", Type = vDbTypes.Decimal });
		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_at", Type = vDbTypes.DateTime });
		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_by", Type = vDbTypes.Decimal });
		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_aid", Type = vDbTypes.Decimal });
		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_at", Type = vDbTypes.DateTime });
		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_del", Type = vDbTypes.Boolean, Default = "F" });
		//    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_void", Type = vDbTypes.Boolean, Default = "F" });

		//    CreateOrAlterTable(request.BotObj.TableName.ToLower(), _listNamesAndTypes);

		//    return new CreateBotFormTableResponse();
		//}

		//private int CreateOrAlterTable(string tableName, List<TableColumnMeta> listNamesAndTypes)
		//{
		//    //checking for space in column name, table name
		//    foreach (TableColumnMeta entry in listNamesAndTypes)
		//    {
		//        if (entry.Name.Contains(CharConstants.SPACE) || tableName.Contains(CharConstants.SPACE))
		//            return -1;
		//    }
		//    var isTableExists = this.EbConnectionFactory.ObjectsDB.IsTableExists(this.EbConnectionFactory.ObjectsDB.IS_TABLE_EXIST, new DbParameter[] { this.EbConnectionFactory.ObjectsDB.GetNewParameter("tbl", EbDbTypes.String, tableName) });
		//    if (!isTableExists)
		//    {
		//        string cols = string.Join(CharConstants.COMMA + CharConstants.SPACE.ToString(), listNamesAndTypes.Select(x => x.Name + CharConstants.SPACE + x.Type.VDbType.ToString() + (x.Default.IsNullOrEmpty() ? "" : (" DEFAULT '" + x.Default + "'"))).ToArray());
		//        string sql = string.Empty;
		//        if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.ORACLE)////////////
		//        {
		//            sql = "CREATE TABLE @tbl(id NUMBER(10), @cols)".Replace("@cols", cols).Replace("@tbl", tableName);
		//            this.EbConnectionFactory.ObjectsDB.CreateTable(sql);//Table Creation
		//            CreateSquenceAndTrigger(tableName);//
		//        }
		//        else if(this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.PGSQL)
		//        {
		//            sql = "CREATE TABLE @tbl( id SERIAL PRIMARY KEY, @cols)".Replace("@cols", cols).Replace("@tbl", tableName);
		//            this.EbConnectionFactory.ObjectsDB.CreateTable(sql);
		//        }
		//        else if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.MYSQL)
		//        {
		//            sql = "CREATE TABLE @tbl( id INTEGER AUTO_INCREMENT PRIMARY KEY, @cols)".Replace("@cols", cols).Replace("@tbl", tableName);
		//            this.EbConnectionFactory.ObjectsDB.CreateTable(sql);
		//        }
		//        return 0;
		//    }
		//    else
		//    {
		//        var colSchema = this.EbConnectionFactory.ObjectsDB.GetColumnSchema(tableName);
		//        string sql = string.Empty;
		//        foreach (TableColumnMeta entry in listNamesAndTypes)
		//        {
		//            bool isFound = false;
		//            foreach (EbDataColumn dr in colSchema)
		//            {
		//                if (entry.Name.ToLower() == (dr.ColumnName.ToLower()))
		//                {
		//                    isFound = true;
		//                    break;
		//                }
		//            }
		//            if (!isFound)
		//            {
		//                sql += entry.Name + " " + entry.Type.VDbType.ToString() + " " + (entry.Default.IsNullOrEmpty() ? "" : (" DEFAULT '" + entry.Default + "'")) + ",";
		//            }
		//        }
		//        bool appendId = false;
		//        var existingIdCol = colSchema.FirstOrDefault(o => o.ColumnName.ToLower() == "id");
		//        if (existingIdCol == null)
		//            appendId = true;
		//        if (!sql.IsEmpty() || appendId)
		//        {
		//            if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.ORACLE)/////////////////////////
		//            {
		//                sql = (appendId ? "id NUMBER(10)," : "") + sql;
		//                if (!sql.IsEmpty())
		//                {
		//                    sql = "ALTER TABLE @tbl ADD (" + sql.Substring(0, sql.Length - 1) + ")";
		//                    sql = sql.Replace("@tbl", tableName);
		//                    this.EbConnectionFactory.ObjectsDB.UpdateTable(sql);
		//                    if (appendId)
		//                        CreateSquenceAndTrigger(tableName);
		//                }
		//            }
		//            else if(this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.PGSQL)
		//            {
		//                sql = (appendId ? "id SERIAL PRIMARY KEY," : "") + sql;
		//                if (!sql.IsEmpty())
		//                {
		//                    sql = "ALTER TABLE @tbl ADD COLUMN " + (sql.Substring(0, sql.Length - 1)).Replace(",", ", ADD COLUMN ");
		//                    sql = sql.Replace("@tbl", tableName);
		//                    this.EbConnectionFactory.ObjectsDB.UpdateTable(sql);
		//                }
		//            }
		//            else if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.MYSQL)
		//            {
		//                sql = (appendId ? "id INTEGER AUTO_INCREMENT PRIMARY KEY," : "") + sql;
		//                if (!sql.IsEmpty())
		//                {
		//                    sql = "ALTER TABLE @tbl ADD COLUMN " + (sql.Substring(0, sql.Length - 1)).Replace(",", ", ADD COLUMN ");
		//                    sql = sql.Replace("@tbl", tableName);
		//                    this.EbConnectionFactory.ObjectsDB.UpdateTable(sql);
		//                }
		//            }
		//            return (0);
		//        }
		//    }
		//    return -1;
		//}

		//private void CreateSquenceAndTrigger(string tableName)
		//{
		//    string sqnceSql = "CREATE SEQUENCE @name_sequence".Replace("@name", tableName);
		//    string trgrSql = string.Format(@"CREATE OR REPLACE TRIGGER {0}_on_insert
		//					BEFORE INSERT ON {0}
		//					FOR EACH ROW
		//					BEGIN
		//						SELECT {0}_sequence.nextval INTO :new.id FROM dual;
		//					END;", tableName);
		//    this.EbConnectionFactory.ObjectsDB.CreateTable(sqnceSql);//Sequence Creation
		//    this.EbConnectionFactory.ObjectsDB.CreateTable(trgrSql);//Trigger Creation
		//}

		//public object Any123(CreateBotFormTableRequest request)
		//{
		//	DbParameter[] parameter1 = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("tbl", EbDbTypes.String, request.BotObj.TableName.ToLower()) };
		//	var rslt = this.EbConnectionFactory.ObjectsDB.IsTableExists(this.EbConnectionFactory.ObjectsDB.IS_TABLE_EXIST, parameter1);
		//	string cols = "";
		//	var Columns = new DVColumnCollection();
		//	string dvRefidOfCards = null;
		//	var pos = 1;
		//	var vDbTypes = this.EbConnectionFactory.ObjectsDB.VendorDbTypes;
		//	string colsName = "id";
		//	Columns.Add(new DVNumericColumn { Data = 0, Name = "id", sTitle = "id", Type = EbDbTypes.Int32, bVisible = true, sWidth = "100px", ClassName = "tdheight" });
		//	foreach (EbControl control in request.BotObj.Controls)
		//	{
		//		DVBaseColumn _col = null;
		//		colsName += "," + control.Name;
		//		if (control is EbNumeric)
		//		{
		//			cols += control.Name + " " + vDbTypes.Decimal.VDbType.ToString() + ",";
		//			_col = new DVNumericColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.Int32, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
		//		}
		//		else if (control is EbTextBox)
		//		{
		//			cols += control.Name + " " + vDbTypes.String.VDbType.ToString() + ",";
		//			_col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.String, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
		//		}
		//		else if (control is EbDate)
		//		{
		//			if ((control as EbDate).EbDateType == EbDateType.Date)
		//			{
		//				cols += control.Name + " " + vDbTypes.Date.VDbType.ToString() + ",";
		//				_col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.Date, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
		//			}
		//			else if ((control as EbDate).EbDateType == EbDateType.DateTime)
		//			{
		//				cols += control.Name + " " + vDbTypes.DateTime.VDbType.ToString() + ",";
		//				_col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.DateTime, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
		//			}
		//			else if ((control as EbDate).EbDateType == EbDateType.Time)
		//			{
		//				cols += control.Name + " " + vDbTypes.Time.VDbType.ToString() + ",";
		//				_col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.Time, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
		//			}
		//		}
		//		else if (control is EbInputGeoLocation)
		//		{
		//			cols += control.Name + " " + vDbTypes.String.VDbType.ToString() + ",";
		//			_col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.String, bVisible = true, sWidth = "100px", ClassName = "dt-body-right tdheight", RenderAs = StringRenderType.Marker };
		//		}

		//		else if (control is EbCardSetParent)
		//		{

		//			if (true)///////(control as EbCardSetParent).MultiSelect// temp fix, bcoz unable to detect singleselect on insert rqst
		//			{
		//				cols += control.Name + " " + vDbTypes.String.VDbType.ToString() + ",";
		//				_col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.String, bVisible = true, sWidth = "100px", ClassName = "dt-body-right tdheight", RenderAs = StringRenderType.Link };

		//				var LineColumns = new DVColumnCollection();
		//				DbParameter[] lineparameter = { this.EbConnectionFactory.ObjectsDB.GetNewParameter("tbl", EbDbTypes.String, request.BotObj.TableName.ToLower() + "_lines") };
		//				var linerslt = this.EbConnectionFactory.ObjectsDB.IsTableExists(this.EbConnectionFactory.ObjectsDB.IS_TABLE_EXIST, lineparameter);
		//				string linecolsName = "id";
		//				LineColumns.Add(new DVNumericColumn { Data = 0, Name = "id", sTitle = "id", Type = EbDbTypes.Int32, bVisible = true, sWidth = "100px", ClassName = "tdheight" });
		//				string linecols = "";
		//				var linepos = 1;

		//				foreach (EbCardField CardField in (control as EbCardSetParent).CardFields)
		//				{
		//					if (!CardField.DoNotPersist)
		//					{
		//						linecolsName += "," + CardField.Name;
		//						DVBaseColumn _linecol = null;
		//						if (CardField is EbCardNumericField)
		//						{
		//							linecols += "," + CardField.Name + " " + vDbTypes.Decimal.VDbType.ToString();
		//							_linecol = new DVNumericColumn { Data = linepos, Name = CardField.Name, sTitle = CardField.Name, Type = EbDbTypes.Double, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
		//						}
		//						else
		//						{
		//							linecols += "," + CardField.Name + " " + vDbTypes.String.VDbType.ToString();
		//							_linecol = new DVStringColumn { Data = linepos, Name = CardField.Name, sTitle = CardField.Name, Type = CardField.EbDbType, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
		//						}
		//						LineColumns.Add(_linecol);
		//						linepos++;
		//					}
		//				}

		//				if (!linerslt)
		//				{
		//					string sql2 = "CREATE TABLE @tbl( id SERIAL PRIMARY KEY, formid integer, selectedcardid integer @cols)".Replace("@cols", linecols).Replace("@tbl", request.BotObj.TableName + "_lines");
		//					this.EbConnectionFactory.ObjectsDB.CreateTable(sql2);
		//					dvRefidOfCards = CreateDsAndDv4Cards(request, LineColumns, linecolsName);
		//				}
		//				else
		//				{
		//					var ColsColl2 = this.EbConnectionFactory.ObjectsDB.GetColumnSchema(request.BotObj.TableName + "_lines");
		//					bool modify2 = false;
		//					var sql2 = "";
		//					var name2 = "";
		//					foreach (DVBaseColumn col in LineColumns)
		//					{
		//						var flag2 = false;
		//						name2 = col.Name.ToLower();
		//						foreach (EbDataColumn dr in ColsColl2)
		//						{
		//							if (name2 == (dr.ColumnName.ToLower()))
		//							{
		//								//type check
		//								//if (col.Type == dr.Type)
		//								//{
		//									flag2 = true;
		//									break;
		//								//}
		//								//else
		//								//{
		//								//	flag2 = true;
		//								//	//Errorrrrrrrrr...........
		//								//}
		//							}
		//							else
		//								flag2 = false;
		//						}
		//						if (!flag2 && !name2.Equals("id"))/////////////
		//						{
		//							sql2 += name2 + " " + vDbTypes.GetVendorDbType(col.Type).ToString() + ",";
		//							modify2 = true;
		//						}
		//						//if (!flag2)
		//						//	sql2 += "alter table @tbl Add column " + name2 + " " + vDbTypes.GetVendorDbType(col.Type).ToString() + ";";

		//					}
		//					if (modify2)
		//					{
		//						if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.ORACLE)/////////////////////////
		//							sql2 = "ALTER TABLE @tbl ADD (" + sql2.Substring(0, sql2.Length - 1) + ")";
		//						else
		//							sql2 = "ALTER TABLE @tbl ADD COLUMN " + sql2.Substring(0, sql2.Length - 1);

		//						sql2 = sql2.Replace("@tbl", request.BotObj.TableName + "_lines");
		//						var ret = this.EbConnectionFactory.ObjectsDB.UpdateTable(sql2);
		//					}
		//					//if (sql2 != "")
		//					//{
		//					//	sql2 = sql2.Replace("@tbl", request.BotObj.TableName + "_lines");
		//					//	var ret2 = this.EbConnectionFactory.ObjectsDB.UpdateTable(sql2);
		//					//}
		//				}
		//			}
		//			//single card
		//			else
		//			{
		//				foreach (EbCardField CardField in (control as EbCardSetParent).CardFields)
		//				{
		//					if (!CardField.DoNotPersist)
		//					{
		//						if (CardField is EbCardNumericField)
		//						{
		//							cols += CardField.Name + " " + vDbTypes.Decimal.VDbType.ToString() + ",";
		//							_col = new DVNumericColumn { Data = pos, Name = CardField.Name, sTitle = CardField.Name, Type = EbDbTypes.Double, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
		//						}
		//						else
		//						{
		//							cols += CardField.Name + " " + vDbTypes.String.VDbType.ToString() + ",";
		//							_col = new DVStringColumn { Data = pos, Name = CardField.Name, sTitle = CardField.Name, Type = CardField.EbDbType, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
		//						}
		//						Columns.Add(_col);
		//						pos++;
		//					}
		//				}

		//			}
		//		}
		//		else
		//		{
		//			cols += control.Name + " " + vDbTypes.String.VDbType.ToString() + ",";
		//			_col = new DVStringColumn { Data = pos, Name = control.Name, sTitle = control.Name, Type = EbDbTypes.String, bVisible = true, sWidth = "100px", ClassName = "tdheight" };
		//		}
		//		Columns.Add(_col);
		//		pos++;
		//	}

		//	colsName += ", eb_created_by, eb_created_at, eb_lastmodified_by, eb_lastmodified_at, eb_del, eb_void, eb_transaction_date, eb_autogen";

		//	Columns.Add(new DVNumericColumn { Data = pos++, Name = "eb_created_by", sTitle = "eb_created_by", Type = EbDbTypes.Decimal, bVisible = true, sWidth = "100px", ClassName = "tdheight" });
		//	Columns.Add(new DVNumericColumn { Data = pos++, Name = "eb_created_at", sTitle = "eb_created_at", Type = EbDbTypes.DateTime, bVisible = true, sWidth = "100px", ClassName = "tdheight" });
		//	Columns.Add(new DVNumericColumn { Data = pos++, Name = "eb_lastmodified_by", sTitle = "eb_lastmodified_by", Type = EbDbTypes.Decimal, bVisible = true, sWidth = "100px", ClassName = "tdheight" });
		//	Columns.Add(new DVNumericColumn { Data = pos++, Name = "eb_lastmodified_at", sTitle = "eb_lastmodified_at", Type = EbDbTypes.DateTime, bVisible = true, sWidth = "100px", ClassName = "tdheight" });
		//	Columns.Add(new DVNumericColumn { Data = pos++, Name = "eb_del", sTitle = "eb_del", Type = EbDbTypes.Boolean, bVisible = true, sWidth = "100px", ClassName = "tdheight" });
		//	Columns.Add(new DVNumericColumn { Data = pos++, Name = "eb_void", sTitle = "eb_void", Type = EbDbTypes.Boolean, bVisible = true, sWidth = "100px", ClassName = "tdheight" });
		//	Columns.Add(new DVNumericColumn { Data = pos++, Name = "eb_transaction_date", sTitle = "eb_transaction_date", Type = EbDbTypes.DateTime, bVisible = true, sWidth = "100px", ClassName = "tdheight" });
		//	Columns.Add(new DVNumericColumn { Data = pos++, Name = "eb_autogen", sTitle = "eb_autogen", Type = EbDbTypes.Decimal, bVisible = true, sWidth = "100px", ClassName = "tdheight" });

		//	if (!rslt)
		//	{
		//		var str = "";
		//		if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.PGSQL)/////////////
		//			str = "id SERIAL PRIMARY KEY,";				
		//		cols = str + cols;
		//		str = "eb_created_by " + vDbTypes.Decimal.VDbType.ToString() + ",";
		//		str += "eb_created_at " + vDbTypes.DateTime.VDbType.ToString() + ",";
		//		str += "eb_lastmodified_by " + vDbTypes.Decimal.VDbType.ToString() + ",";
		//		str += "eb_lastmodified_at " + vDbTypes.DateTime.VDbType.ToString() + ",";
		//		str += "eb_del " + vDbTypes.Boolean.VDbType.ToString() + " DEFAULT 'F',";
		//		str += "eb_void " + vDbTypes.Boolean.VDbType.ToString() + " DEFAULT 'F',";
		//		str += "eb_transaction_date " + vDbTypes.DateTime.VDbType.ToString() + ",";
		//		str += "eb_autogen " + vDbTypes.Decimal.VDbType.ToString();
		//		cols += str;
		//		string sql = "CREATE TABLE @tbl(@cols)".Replace("@cols", cols).Replace("@tbl", request.BotObj.TableName);
		//		this.EbConnectionFactory.ObjectsDB.CreateTable(sql);

		//		if (!dvRefidOfCards.IsNullOrEmpty())
		//		{
		//			foreach (DVBaseColumn col in Columns)
		//			{
		//				if (col is DVStringColumn && (col as DVStringColumn).RenderAs == StringRenderType.Link)
		//				{
		//					(col as DVStringColumn).LinkRefId = dvRefidOfCards;
		//				}
		//			}
		//		}
		//		CreateDsAndDv(request, Columns, colsName);

		//	}

		//	//Alter Table
		//	else
		//	{
		//		//string sql = @"select column_name,data_type from information_schema.columns
		//		//                where table_name = '@tbl';".Replace("@tbl", request.BotObj.TableName);
		//		//EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql);
		//		var ColsColl = this.EbConnectionFactory.ObjectsDB.GetColumnSchema(request.BotObj.TableName);
		//		bool modify = false;
		//		var sql = string.Empty;								
		//		var name = string.Empty;
		//		foreach (DVBaseColumn col in Columns)
		//		{
		//			var flag = false;
		//			name = col.Name.ToLower();
		//			foreach (EbDataColumn dr in ColsColl)
		//			{
		//				if (name == (dr.ColumnName.ToLower()))
		//				{
		//					//type check
		//					//if (col.Type == dr.Type)
		//					//{
		//						flag = true;
		//						break;
		//					//}
		//					//else
		//					//{
		//					//	flag = true;
		//					//	//Errorrrrrrrrr...........
		//					//}
		//				}
		//				else
		//					flag = false;
		//			}
		//			if (!flag && !name.Equals("id"))/////////////
		//			{
		//				sql += name + " " + vDbTypes.GetVendorDbType(col.Type).ToString() + ",";
		//				modify = true;
		//			}

		//		}
		//		if (modify)
		//		{
		//			if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.ORACLE)/////////////////////////
		//				sql = "ALTER TABLE @tbl ADD (" + sql.Substring(0, sql.Length - 1) + ")";
		//			else
		//				sql = "ALTER TABLE @tbl ADD COLUMN " + sql.Substring(0, sql.Length - 1);

		//			sql = sql.Replace("@tbl", request.BotObj.TableName);
		//			var ret = this.EbConnectionFactory.ObjectsDB.UpdateTable(sql);
		//		}
		//	}

		//	return new CreateBotFormTableResponse();
		//}

		//public void CreateDsAndDv(CreateBotFormTableRequest request, DVColumnCollection Columns, string ColumnName)
		//{
		//    var dsobj = new EbDataReader();
		//    dsobj.Sql = "SELECT @colname@ FROM @tbl".Replace("@tbl", request.BotObj.TableName).Replace("@colname@", ColumnName);
		//    var ds = new EbObject_Create_New_ObjectRequest();
		//    ds.Name = request.BotObj.Name + "_datasource";
		//    ds.Description = "desc";
		//    ds.Json = EbSerializers.Json_Serialize(dsobj);
		//    ds.Status = ObjectLifeCycleStatus.Live;
		//    ds.Relations = "";
		//    ds.IsSave = false;
		//    ds.Tags = "";
		//    ds.Apps = request.Apps;
		//    ds.SolnId = request.SolnId;
		//    ds.WhichConsole = request.WhichConsole;
		//    ds.UserId = request.UserId;
		//    var myService = base.ResolveService<EbObjectService>();
		//    var res = myService.Post(ds);
		//    var refid = res.RefId;

		//    var dvobj = new EbTableVisualization();
		//    dvobj.DataSourceRefId = refid;
		//    dvobj.Columns = Columns;
		//    dvobj.DSColumns = Columns;
		//    var ds1 = new EbObject_Create_New_ObjectRequest();
		//    ds1.Name = request.BotObj.Name + "_response";
		//    ds1.Description = "desc";
		//    ds1.Json = EbSerializers.Json_Serialize(dvobj);
		//    ds1.Status = ObjectLifeCycleStatus.Live;
		//    ds1.Relations = refid;
		//    ds1.IsSave = false;
		//    ds1.Tags = "";
		//    ds1.Apps = request.Apps;
		//    ds1.SolnId = request.SolnId;
		//    ds1.WhichConsole = request.WhichConsole;
		//    ds1.UserId = request.UserId;
		//    var res1 = myService.Post(ds1);
		//    var refid1 = res.RefId;
		//}

		//public string CreateDsAndDv4Cards(CreateBotFormTableRequest request, DVColumnCollection Columns, string ColumnName)
		//{
		//    var dsobj = new EbDataReader();
		//    dsobj.Sql = "SELECT @ColumnName@ FROM @tbl WHERE formid = @id".Replace("@tbl", request.BotObj.TableName + "_lines").Replace("@ColumnName@", ColumnName);
		//    var ds = new EbObject_Create_New_ObjectRequest();
		//    ds.Name = request.BotObj.Name + "_datasource4Card";
		//    ds.Description = "desc";
		//    ds.Json = EbSerializers.Json_Serialize(dsobj);
		//    ds.Status = ObjectLifeCycleStatus.Live;
		//    ds.Relations = "";
		//    ds.IsSave = false;
		//    ds.Tags = "";
		//    ds.Apps = request.Apps;
		//    ds.SolnId = request.SolnId;
		//    ds.WhichConsole = request.WhichConsole;
		//    ds.UserId = request.UserId;
		//    var myService = base.ResolveService<EbObjectService>();
		//    var res = myService.Post(ds);
		//    var refid = res.RefId;

		//    var dvobj = new EbTableVisualization();
		//    dvobj.DataSourceRefId = refid;
		//    dvobj.Columns = Columns;
		//    dvobj.DSColumns = Columns;
		//    var ds1 = new EbObject_Create_New_ObjectRequest();
		//    ds1.Name = request.BotObj.Name + "_response4Card";
		//    ds1.Description = "desc";
		//    ds1.Json = EbSerializers.Json_Serialize(dvobj);
		//    ds1.Status = ObjectLifeCycleStatus.Live;
		//    ds1.Relations = refid;
		//    ds1.IsSave = false;
		//    ds1.Tags = "";
		//    ds1.Apps = request.Apps;
		//    ds1.SolnId = request.SolnId;
		//    ds1.WhichConsole = request.WhichConsole;
		//    ds1.UserId = request.UserId;
		//    var res1 = myService.Post(ds1);
		//    //var refid1 = res1.RefId;	

		//    return res1.RefId;
		//}

		public object Any(InsertIntoBotFormTableRequest request)
		{
			DbParameter parameter1;
			List<DbParameter> paramlist = new List<DbParameter>();
			string cols = "";
			string vals = "";
			string colvals = string.Empty;

			string qry = string.Empty;
			int rslt = 0;

			DbParameter Param4Lines;
			string Cols4Lines = "";
			List<string> Vals4Lines = new List<string>(); ;
			int cardCount = 0;


			foreach (var obj in request.Fields)
			{
				if (obj.Type == EbDbTypes.Decimal)
				{
					cols += obj.Name + ",";
					if (obj.AutoIncrement)
					{
						vals += string.Format("(SELECT MAX({0})+1 FROM {1}),", obj.Name, request.TableName);
					}
					else
					{
						vals += "@" + obj.Name + ",";
						colvals += obj.Name + " = @" + obj.Name + ", ";
						parameter1 = this.EbConnectionFactory.ObjectsDB.GetNewParameter(obj.Name, EbDbTypes.Decimal, double.Parse(obj.Value));
						paramlist.Add(parameter1);
					}
				}
				else if (obj.Type == EbDbTypes.Date)
				{
					cols += obj.Name + ",";
					colvals += obj.Name + " = @" + obj.Name + ", ";
					vals += "@" + obj.Name + ",";
					parameter1 = this.EbConnectionFactory.ObjectsDB.GetNewParameter(obj.Name, EbDbTypes.Date, DateTime.Parse(obj.Value));
					paramlist.Add(parameter1);
				}
				else if (obj.Type == EbDbTypes.DateTime)
				{
					cols += obj.Name + ",";
					colvals += obj.Name + " = @" + obj.Name + ", ";
					vals += "@" + obj.Name + ",";
					parameter1 = this.EbConnectionFactory.ObjectsDB.GetNewParameter(obj.Name, EbDbTypes.DateTime, DateTime.Parse(obj.Value));
					paramlist.Add(parameter1);
				}
				else if (obj.Type == EbDbTypes.Time)
				{
					cols += obj.Name + ",";
					vals += "@" + obj.Name + ",";
					colvals += obj.Name + " = @" + obj.Name + ", ";
					DateTime dt = DateTime.Parse(obj.Value);
					//DateTime.ParseExact(obj.Value, "HH:mm:ss", CultureInfo.InvariantCulture)
					parameter1 = this.EbConnectionFactory.ObjectsDB.GetNewParameter(obj.Name, EbDbTypes.Time, dt.ToString("HH:mm:ss"));
					paramlist.Add(parameter1);
				}
				else if (obj.Type == EbDbTypes.Json)
				{
					Dictionary<int, List<BotFormField>> ObjectLines = JsonConvert.DeserializeObject<Dictionary<int, List<BotFormField>>>(obj.Value);
					if (ObjectLines.Keys.Count > 0)
					{
						cols += obj.Name + ",";
						vals += "@" + obj.Name + ",";
						colvals += obj.Name + " = @" + obj.Name + ", ";
						parameter1 = this.EbConnectionFactory.ObjectsDB.GetNewParameter(obj.Name, EbDbTypes.String, "LINES");
						paramlist.Add(parameter1);
					}

					//Insert to table _lines
					foreach (KeyValuePair<int, List<BotFormField>> card in ObjectLines)
					{
						// do something with card.Value or card.Key
						string Vals4SingleLine = "";
						foreach (var objLines in card.Value)
						{
							if (cardCount == 0)
								Cols4Lines += objLines.Name + ",";
							if (objLines.Type == EbDbTypes.Double)
							{
								Vals4SingleLine += "@line" + cardCount + objLines.Name + ",";
								Param4Lines = this.EbConnectionFactory.ObjectsDB.GetNewParameter("line" + cardCount + objLines.Name, EbDbTypes.Double, double.Parse(objLines.Value));
							}
							else
							{
								Vals4SingleLine += "@line" + cardCount + objLines.Name + ",";
								Param4Lines = this.EbConnectionFactory.ObjectsDB.GetNewParameter("line" + cardCount + objLines.Name, EbDbTypes.String, objLines.Value);
							}
							paramlist.Add(Param4Lines);
						}
						if (cardCount == 0)
							Cols4Lines += "itemid";
						Vals4SingleLine += "@line" + cardCount + "_item_id";
						paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("line" + cardCount + "_item_id", EbDbTypes.Int32, card.Key));
						Vals4Lines.Add(Vals4SingleLine);
						cardCount++;
					}
				}
				else
				{
					cols += obj.Name + ",";
					vals += "@" + obj.Name + ",";
					colvals += obj.Name + " = @" + obj.Name + ", ";
					parameter1 = this.EbConnectionFactory.ObjectsDB.GetNewParameter(obj.Name, EbDbTypes.String, obj.Value);
					paramlist.Add(parameter1);
				}

			}

			if (request.Id == 0)
			{
				cols = cols + "eb_created_by, eb_created_aid, eb_created_at, eb_lastmodified_by, eb_lastmodified_aid, eb_lastmodified_at";
				vals = vals + "@eb_created_by,@eb_created_aid,@eb_created_at,@eb_lastmodified_by,@eb_lastmodified_aid,@eb_lastmodified_at";
				paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_created_by", EbDbTypes.Int32, request.UserId));
				paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_created_aid", EbDbTypes.Int32, request.AnonUserId));
				paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_created_at", EbDbTypes.DateTime, DateTime.Now));
				paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_lastmodified_by", EbDbTypes.Int32, request.UserId));
				paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_lastmodified_aid", EbDbTypes.Int32, request.AnonUserId));
				paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_lastmodified_at", EbDbTypes.DateTime, DateTime.Now));
				qry = string.Format("insert into @tbl({0}) values({1})".Replace("@tbl", request.TableName), cols, vals);

				//append second insert query
				if (!Cols4Lines.IsNullOrEmpty())
				{
					qry = "WITH rows AS (" + qry + " returning id)";
					qry += "INSERT INTO " + request.TableName + "_lines(formid," + Cols4Lines + ") ";

					qry += "(SELECT rows.id," + Vals4Lines[0] + " FROM rows)";
					for (int i = 1; i < Vals4Lines.Count; i++)
					{
						qry += " UNION ALL (SELECT rows.id," + Vals4Lines[i] + " FROM rows)";
					}
				}
				rslt = this.EbConnectionFactory.ObjectsDB.InsertTable(qry, paramlist.ToArray());
			}
			else
			{
				colvals += "eb_lastmodified_by = @eb_lastmodified_by, eb_lastmodified_at = @eb_lastmodified_at";

				paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, request.Id));
				paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_lastmodified_by", EbDbTypes.Int32, request.UserId));
				paramlist.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("eb_lastmodified_at", EbDbTypes.DateTime, DateTime.Now));

				qry = string.Format("UPDATE {0} SET {1} WHERE id = @id ;", request.TableName, colvals);
				rslt = this.EbConnectionFactory.ObjectsDB.UpdateTable(qry, paramlist.ToArray());
			}
			return new InsertIntoBotFormTableResponse { RowAffected = rslt };
		}


		public SubmitBotFormResponse Any(SubmitBotFormRequest request)
		{
			var myService = base.ResolveService<EbObjectService>();
			var formObj = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
			var FormObj = EbSerializers.Json_Deserialize(formObj.Data[0].Json);
			if (FormObj is EbBotForm)
			{
				var BotForm = FormObj as EbBotForm;
				if (request.Id > 0)
					UpdateBotFormTable(request.Id, BotForm.TableName, request.Fields, request.UserId);
			}

			return new SubmitBotFormResponse { };
		}

		private bool IsFormDataValid(dynamic FormObj, List<BotFormField> Fields)
		{
			var BotForm = FormObj as EbBotForm;
			//var engine = new Jurassic.ScriptEngine();
			foreach (EbControl control in BotForm.Controls)
			{
				var CurFld = Fields.Find(i => i.Name == control.Name);
			}

			return false;
		}


		private int UpdateBotFormTable(int Id, string TableName, List<BotFormField> Fields, int UserId)
		{
			List<BotFormField> UpdateList = new List<BotFormField>();
			List<DbParameter> parameters = new List<DbParameter>();
			int rstatus = 0;
			string cols = string.Empty;
			string colvals = string.Empty;
			foreach (BotFormField item in Fields)
			{
				cols += item.Name + ",";
			}
			string selectQry = string.Format("SELECT {0} FROM {1} WHERE id=@id;", cols.Substring(0, cols.Length - 1), TableName);

			var ds = this.EbConnectionFactory.DataDB.DoQueries(selectQry, new DbParameter[] { this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, Id) });
			if (ds.Tables.Count > 0 && ds.Tables[0].Rows.Count > 0)
			{
				var dr = ds.Tables[0].Rows[0];
				for (int i = 0; i < Fields.Count; i++)
				{
					if (Fields[i].Value != dr[i].ToString())
					{
						UpdateList.Add(new BotFormField
						{
							Name = Fields[i].Name,
							Type = Fields[i].Type,
							Value = Fields[i].Value,
							OldValue = dr[i].ToString()
						});
						colvals += Fields[i].Name + "=@" + Fields[i].Name + ",";
						parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter(Fields[i].Name, Fields[i].Type, Fields[i].Value));
					}
				}
				if (!colvals.IsNullOrEmpty())
				{
					parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, Id));
					string Qry = string.Format("UPDATE {0} SET {1} WHERE id=@id;", TableName, colvals.Substring(0, colvals.Length - 1));
					rstatus = this.EbConnectionFactory.ObjectsDB.UpdateTable(Qry, parameters.ToArray());
				}
				//UpdateLog(UpdateList, TableName, UserId);
			}
			return rstatus;
		}

		public void UpdateLog(List<BotFormField> _Fields, string _FormId, int _UserId)
		{
			List<DbParameter> parameters = new List<DbParameter>();
			parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("formid", EbDbTypes.String, _FormId));
			parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_createdby", EbDbTypes.Int32, _UserId));
			parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_createdat", EbDbTypes.DateTime, DateTime.Now));
			string Qry = "INSERT INTO eb_audit_master(formid, eb_createdby, eb_createdat) VALUES (@formid, @eb_createdby, @eb_createdat) RETURNING id;";
			EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery(Qry, parameters.ToArray());
			var id = Convert.ToInt32(dt.Rows[0][0]);

			string lineQry = "INSERT INTO eb_audit_lines(masterid, fieldname, oldvalue, newvalue) VALUES ";
			List<DbParameter> parameters1 = new List<DbParameter>();
			parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter("masterid", EbDbTypes.Int32, id));
			for (int i = 0; i < _Fields.Count; i++)
			{
				lineQry += "(@masterid, @" + _Fields[i].Name + ", @old" + _Fields[i].Name + ", @new" + _Fields[i].Name + "),";
				parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter(_Fields[i].Name, EbDbTypes.String, _Fields[i].Name));
				parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter("new" + _Fields[i].Name, EbDbTypes.String, _Fields[i].Value));
				parameters1.Add(this.EbConnectionFactory.DataDB.GetNewParameter("old" + _Fields[i].Name, EbDbTypes.String, _Fields[i].OldValue));
			}
			var rrr = this.EbConnectionFactory.ObjectsDB.DoNonQuery(lineQry.Substring(0, lineQry.Length - 1), parameters1.ToArray());
		}

		public GetBotsResponse Get(GetBotsRequest request)
		{
			List<BotDetails> list = new List<BotDetails>();
			string qry = "";
			DbParameter parameters = null;
			EbDataTable table = null;
			try
			{
				if (request.Id_lst.IsNullOrEmpty())
				{
					qry = @"SELECT id, applicationname, app_icon,description, app_settings FROM eb_applications WHERE application_type = 3 AND eb_del = 'F'";
					table = this.EbConnectionFactory.DataDB.DoQuery(qry);
				}
				else
				{
					int[] idlst = new int[] { };
					idlst = request.Id_lst.Split('-').Select(x => int.Parse(x)).ToArray();
					
					qry = string.Format(@"SELECT id, applicationname, app_icon,description, app_settings FROM eb_applications WHERE application_type = 3 AND eb_del = 'F' AND id = ANY (ARRAY[{0}])", string.Join(",", idlst));
					//parameters = this.EbConnectionFactory.ObjectsDB.GetNewParameter("idlst", EbDbTypes.Int32, idlst[0]);
					//table = this.EbConnectionFactory.DataDB.DoQuery(qry, parameters);
				}

				table = this.EbConnectionFactory.DataDB.DoQuery(qry);
				foreach (EbDataRow row in table.Rows)
				{
					list.Add(new BotDetails
					{
						id = Convert.ToInt32(row[0]),
						name = row[1].ToString(),
						icon = row[2].ToString(),
						Description = row[3].ToString(),
						botsettings = JsonConvert.DeserializeObject<EbBotSettings>(row[4].ToString())
					});
				}
			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
			}
			return new GetBotsResponse { BotList = list };
		}

		public GetBotSettingsResponse Get(GetBotSettingsRequest request)
		{
			string sql = "SELECT app_settings FROM eb_applications WHERE id = @appid AND application_type = 3 AND eb_del = 'F'";
			DbParameter[] parameters = new DbParameter[] {
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("appid", EbDbTypes.Int32, request.AppId)
			};
			//this.Redis.Set("","");
			EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql, parameters);
			return new GetBotSettingsResponse()
			{
				Settings = JsonConvert.DeserializeObject<EbBotSettings>(dt.Rows[0][0].ToString())
			};
		}

		public RedisBotSettingsResponse Post(RedisBotSettingsRequest request)
		{
			RedisBotSettingsResponse stgRes = new RedisBotSettingsResponse();

			try
			{
				this.EbConnectionFactory = new EbConnectionFactory(request.SolnId, this.Redis);
				string sql = @"SELECT app_settings FROM eb_applications  WHERE id = @appid AND application_type = @apptype AND eb_del='F';";
				DbParameter[] parameters = new DbParameter[] {
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("appid", EbDbTypes.Int32, request.AppId),
				this.EbConnectionFactory.ObjectsDB.GetNewParameter("apptype", EbDbTypes.Int32, request.AppType)
					};
				EbDataTable dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql, parameters);
				if (dt.Rows.Count > 0)
				{
					stgRes.Settings = dt.Rows[0][0].ToString();
					this.Redis.Set<EbBotSettings>(string.Format("{0}-{1}_app_settings", request.SolnId, request.AppId), JsonConvert.DeserializeObject<EbBotSettings>(stgRes.Settings));
					stgRes.ResStatus = 1;
				}
				else
				{
					stgRes.ResStatus = 0;
				}
			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
			}
			return stgRes;
		}

	}
}
