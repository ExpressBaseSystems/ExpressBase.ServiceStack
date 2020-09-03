using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Linq;
using ExpressBase.Security;
using ExpressBase.Common.Extensions;
using System.Data.Common;
using ExpressBase.Common.Application;
using Newtonsoft.Json;
using ServiceStack;
using ExpressBase.Objects.Helpers;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class MobileServices : EbBaseService
    {
        public MobileServices(IEbConnectionFactory _dbf, IMessageProducer _mqp) : base(_dbf, _mqp) { }

        public CreateMobileFormTableResponse Post(CreateMobileFormTableRequest request)
        {
            CreateMobileFormTableResponse response = new CreateMobileFormTableResponse();
            string msg = string.Empty;
            try
            {
                EbMobileForm mobileForm = (EbMobileForm)request.MobilePage.Container;
                IVendorDbTypes vDbTypes = this.EbConnectionFactory.DataDB.VendorDbTypes;
                var tableMetaDict = mobileForm.GetTableMetaCollection(vDbTypes);

                WebFormServices webservices = base.ResolveService<WebFormServices>();

                int status = 0;
                foreach (var pair in tableMetaDict)
                {
                    int s = webservices.CreateOrAlterTable(pair.Key, pair.Value, ref msg);
                    if (pair.Key.Equals(mobileForm.TableName))
                        status = s;
                }

                //deploy webform if create 
                if (status == 0)
                    this.DeployWebForm(request);

                if (mobileForm.AutoDeployMV)
                    this.DeployMobileVis(request, tableMetaDict[mobileForm.TableName]);
            }
            catch (Exception e)
            {
                Console.WriteLine("EXCEPTION AT MOBILE TABLE CREATION : " + e.Message);
                Console.WriteLine(e.StackTrace);
            }
            return response;
        }

        private void DeployMobileVis(CreateMobileFormTableRequest request, List<TableColumnMeta> tablemeta)
        {
            string autgenref = (request.MobilePage.Container as EbMobileForm).AutoGenMVRefid;

            if (string.IsNullOrEmpty(autgenref))
            {
                IEnumerable<TableColumnMeta> _list = tablemeta.Where(x => x.Name != "eb_del" && x.Name != "eb_ver_id" && !(x.Name.Contains("_ebbkup")) && !(x.Control is EbFileUploader));
                string cols = string.Join(CharConstants.COMMA + "\n \t ", _list.Select(x => x.Name).ToArray());

                EbMobilePage vispage = new EbMobilePage
                {
                    Name = $"{request.MobilePage.Name}_list",
                    DisplayName = $"{request.MobilePage.DisplayName} List"
                };

                EbMobileVisualization _vis = new EbMobileVisualization
                {
                    Name = "tab0_visualization_autogen",
                    DataSourceRefId = this.CreateDataReader(request, cols)
                };

                _vis.OfflineQuery = new EbScript { Code = $"SELECT * FROM {(request.MobilePage.Container as EbMobileForm).TableName};" };

                _vis.DataLayout = new EbMobileTableLayout { RowCount = 2, ColumCount = 2 };

                for (int i = 0; i < 2; i++)
                {
                    for (int k = 0; k < 2; k++)
                    {
                        _vis.DataLayout.CellCollection.Add(new EbMobileTableCell
                        {
                            RowIndex = i,
                            ColIndex = k
                        });
                    }
                }
                _vis.DataLayout.CellCollection[0].Width = 60;
                _vis.DataLayout.CellCollection[0].ControlCollection.Add(new EbMobileDataColumn
                {
                    TableIndex = 0,
                    ColumnIndex = 0,
                    ColumnName = _list.ToArray()[0].Name,
                    Type = _list.ToArray()[0].Type.EbDbType
                });
                _vis.SourceFormRefId = (request.MobilePage.Container as EbMobileForm).RefId;
                vispage.Container = _vis;

                string refid = this.CreateNewObjectRequest(request, vispage);
                (request.MobilePage.Container as EbMobileForm).AutoGenMVRefid = refid;
                this.SaveObjectRequest(request, request.MobilePage);
            }
        }

        private void DeployWebForm(CreateMobileFormTableRequest request)
        {
            string autgenref = (request.MobilePage.Container as EbMobileForm).WebFormRefId;

            try
            {
                if (string.IsNullOrEmpty(autgenref))
                {
                    EbMobileForm mobileform = (EbMobileForm)request.MobilePage.Container;

                    EbWebForm webform = new EbWebForm
                    {
                        EbSid = Guid.NewGuid().ToString("N"),
                        Name = request.MobilePage.Name + "_autogen_webform",
                        DisplayName = request.MobilePage.DisplayName + " AutoGen Webform",
                        TableName = mobileform.TableName,
                        Padding = new UISides { Top = 0, Right = 0, Bottom = 0, Left = 0 }
                    };

                    var counter = new Dictionary<string, int>();
                    foreach (EbMobileControl ctrl in mobileform.ChildControls)
                    {
                        string name = ctrl.GetType().Name;

                        if (counter.ContainsKey(name))
                            counter[name]++;
                        else
                            counter[name] = 0;

                        EbControl Wctrl;

                        if (ctrl is EbMobileTableLayout)
                        {
                            foreach (EbMobileTableCell cell in (ctrl as EbMobileTableLayout).CellCollection)
                            {
                                foreach (EbMobileControl tctrl in cell.ControlCollection)
                                {
                                    Wctrl = ctrl.GetWebFormCtrl(counter[name]);
                                    webform.Controls.Add(Wctrl);
                                }
                            }
                        }
                        else
                        {
                            Wctrl = ctrl.GetWebFormCtrl(counter[name]);
                            webform.Controls.Add(Wctrl);
                        }
                    }

                    string refid = this.CreateNewObjectRequest(request, webform);
                    (request.MobilePage.Container as EbMobileForm).WebFormRefId = refid;
                    this.SaveObjectRequest(request, request.MobilePage);
                }
            }
            catch (Exception ee)
            {
                Console.WriteLine(ee.Message);
            }
        }

        private string CreateDataReader(CreateMobileFormTableRequest request, string cols)
        {
            EbDataReader drObj = new EbDataReader
            {
                Sql = "SELECT \n \t id,@colname@ FROM @tbl \n WHERE eb_del='F'".Replace("@tbl", (request.MobilePage.Container as EbMobileForm).TableName).Replace("@colname@", cols),
                FilterDialogRefId = "",
                Name = request.MobilePage.Name + "_AutoGenDR",
                DisplayName = request.MobilePage.DisplayName + "_AutoGenDR",
                Description = request.MobilePage.Description
            };
            return CreateNewObjectRequest(request, drObj);
        }

        private string CreateNewObjectRequest(CreateMobileFormTableRequest request, EbObject dvobj)
        {
            string _rel_obj_tmp = string.Join(",", dvobj.DiscoverRelatedRefids());
            EbObject_Create_New_ObjectRequest ds1 = (new EbObject_Create_New_ObjectRequest
            {
                Name = dvobj.Name,
                Description = dvobj.Description,
                Json = EbSerializers.Json_Serialize(dvobj),
                Status = ObjectLifeCycleStatus.Live,
                IsSave = false,
                Tags = "",
                Apps = request.Apps,
                SolnId = request.SolnId,
                WhichConsole = request.WhichConsole,
                UserId = request.UserId,
                SourceObjId = "0",
                SourceVerID = "0",
                DisplayName = dvobj.DisplayName,
                SourceSolutionId = request.SolnId,
                Relations = _rel_obj_tmp
            });
            var myService = base.ResolveService<EbObjectService>();
            var res = myService.Post(ds1);
            return res.RefId;
        }

        private void SaveObjectRequest(CreateMobileFormTableRequest request, EbObject obj)
        {
            string _rel_obj_tmp = string.Join(",", obj.DiscoverRelatedRefids());
            EbObject_SaveRequest ds = new EbObject_SaveRequest
            {
                RefId = obj.RefId,
                Name = obj.Name,
                Description = obj.Description,
                Json = EbSerializers.Json_Serialize(obj),
                Relations = _rel_obj_tmp,
                Tags = "",
                Apps = request.Apps,
                DisplayName = obj.DisplayName
            };
            var myService = base.ResolveService<EbObjectService>();
            EbObject_SaveResponse res = myService.Post(ds);
        }

        //get solution data
        public EbMobileSolutionData Get(MobileSolutionDataRequest request)
        {
            EbMobileSolutionData data = new EbMobileSolutionData();

            string idcheck = EbConnectionFactory.DataDB.EB_GET_MOB_MENU_OBJ_IDS;
            const string acquery = @"SELECT 
	                                        EA.id, EA.applicationname,EA.app_icon,EA.application_type,EA.app_settings
                                        FROM 
	                                        eb_applications EA
                                        WHERE
	                                        EXISTS (SELECT * FROM eb_objects2application EOA WHERE EOA.app_id = EA.id AND EOA.eb_del = 'F' {0})
                                        AND
	                                        EA.eb_del = 'F'
                                        AND
	                                        EA.application_type = 2;";

            User UserObject = GetUserObject(request.UserAuthId);
            bool isAdmin = UserObject.IsAdmin();
            string sql;
            EbDataTable dt;

            try
            {
                if (isAdmin)
                {
                    sql = string.Format(acquery, string.Empty);
                    dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql);
                }
                else
                {
                    string[] Ids = UserObject.GetAccessIds();
                    sql = string.Format(acquery, idcheck);
                    DbParameter[] parameters = { this.EbConnectionFactory.DataDB.GetNewParameter("ids", EbDbTypes.String, String.Join(",", Ids)) };
                    dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql, parameters);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("exception at get all application [MobileSolutionDataRequest] ::" + ex.Message);
                dt = new EbDataTable();
            }

            foreach (EbDataRow row in dt.Rows)
            {
                data.Applications.Add(new AppDataToMob
                {
                    AppId = Convert.ToInt32(row["id"]),
                    AppName = row["applicationname"].ToString(),
                    AppIcon = row["app_icon"].ToString(),
                    AppSettings = JsonConvert.DeserializeObject<EbMobileSettings>(row["app_settings"].ToString())
                });
            }

            GetMobilePagesByAppliation(data, UserObject, isAdmin, request.Export);

            return data;
        }


        private void GetMobilePagesByAppliation(EbMobileSolutionData data, User user, bool isAdmin, bool export)
        {
            string idcheck = EbConnectionFactory.ObjectsDB.EB_GET_MOBILE_PAGES;
            string Sql = EbConnectionFactory.ObjectsDB.EB_GET_MOBILE_PAGES_OBJS;

            foreach (AppDataToMob app in data.Applications)
            {
                if (app.AppSettings != null && export)
                {
                    EbDataSet ds = PullAppConfiguredData(app.AppSettings);
                    app.OfflineData.Tables.AddRange(ds.Tables);
                }

                List<DbParameter> parameters = new List<DbParameter> {
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("appid", EbDbTypes.Int32, app.AppId)
                };

                string query;

                if (isAdmin)
                    query = string.Format(Sql, string.Empty);
                else
                {
                    string[] objids = user.GetAccessIds();
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("objids", EbDbTypes.String, objids.Join(",")));
                    query = string.Format(Sql, idcheck);
                }

                try
                {
                    EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(query, parameters.ToArray());

                    foreach (EbDataRow dr in ds.Tables[0].Rows)
                    {
                        EbObjectType objType = (EbObjectType)Convert.ToInt32(dr["obj_type"]);
                        if (objType.IntCode == EbObjectTypes.MobilePage)
                        {
                            app.MobilePages.Add(new MobilePagesWraper
                            {
                                Name = dr["obj_name"].ToString(),
                                DisplayName = dr["display_name"].ToString(),
                                Version = dr["version_num"].ToString(),
                                Json = dr["obj_json"].ToString(),
                                RefId = dr["refid"].ToString()
                            });
                        }
                        else
                        {
                            app.WebObjects.Add(new WebObjectsWraper
                            {
                                Name = dr["obj_name"].ToString(),
                                DisplayName = dr["display_name"].ToString(),
                                Version = dr["version_num"].ToString(),
                                Json = dr["obj_json"].ToString(),
                                RefId = dr["refid"].ToString(),
                                ObjectType = objType.IntCode
                            });
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message);
                }
            }
        }

        private EbDataSet PullAppConfiguredData(EbMobileSettings Settings)
        {
            EbDataSet DataSet = new EbDataSet();

            try
            {
                foreach (DataImportMobile DI in Settings.DataImport)
                {
                    int objtype = Convert.ToInt32(DI.RefId.Split(CharConstants.DASH)[2]);
                    if (objtype == (int)EbObjectTypes.DataReader)
                    {
                        var resp = this.Gateway.Send<DataSourceDataSetResponse>(new DataSourceDataSetRequest
                        {
                            RefId = DI.RefId
                        });

                        if (resp.DataSet.Tables.Any())
                        {
                            resp.DataSet.Tables[0].TableName = DI.TableName;
                            DataSet.Tables.Add(resp.DataSet.Tables[0]);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception at pull app configured data ::" + ex.Message);
            }
            return DataSet;
        }

        public GetMobileVisDataResponse Get(GetMobileVisDataRequest request)
        {
            GetMobileVisDataResponse resp = new GetMobileVisDataResponse();
            try
            {
                EbDataReader _ds = this.Redis.Get<EbDataReader>(request.DataSourceRefId);
                if (_ds == null)
                {
                    EbObjectParticularVersionResponse obj = this.Gateway.Send<EbObjectParticularVersionResponse>(new EbObjectParticularVersionRequest
                    {
                        RefId = request.DataSourceRefId
                    });
                    _ds = EbSerializers.Json_Deserialize<EbDataReader>(obj.Data[0].Json);
                }

                List<DbParameter> parameters = request.Params.ParamsToDbParameters(this.EbConnectionFactory);

                parameters.Add(this.EbConnectionFactory.DataDB.GetNewParameter("eb_currentuser_id", EbDbTypes.Int32, request.UserId));

                if (request.Limit != 0)
                {
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("limit", EbDbTypes.Int32, request.Limit));
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("offset", EbDbTypes.Int32, request.Offset));
                }

                string wraped = WrapQuery(_ds.Sql, request.Limit > 0, request.IsPowerSelect, request.Params, request.SortOrder);

                resp.Data = this.EbConnectionFactory.DataDB.DoQueries(wraped, parameters.ToArray());
            }
            catch (Exception ex)
            {
                resp.Message = "No Data";
                Console.WriteLine("Exception at object list for user mobile req ::" + ex.Message);
            }
            return resp;
        }

        private string WrapQuery(string sql, bool has_limit, bool is_powerselect, List<Param> parameters, List<SortColumn> sort_order)
        {
            string wraped = string.Empty;

            const string EBPARAM_LOCID = "eb_loc_id";

            try
            {
                sql = sql.Trim().TrimEnd(CharConstants.SEMI_COLON);

                if (is_powerselect && parameters.Any())
                {
                    Param p = parameters[0];
                    wraped += $"SELECT * FROM ({sql}) AS PWWRP WHERE LOWER(PWWRP.{p.Name}) LIKE '%{p.Value.ToLower()}%'";
                }
                else
                {
                    List<Param> sqlP = SqlHelper.GetSqlParams(sql, (int)EbObjectTypes.DataReader);
                    List<string> filterList = new List<string>();

                    wraped += $"SELECT * FROM ({sql}) AS PWWRP ";
                    foreach (Param param in parameters)
                    {
                        if (param.Name == EBPARAM_LOCID) continue;

                        Param p = sqlP.Find(item => item.Name == param.Name);
                        if (p == null)
                            filterList.Add($"PWWRP.{param.Name} = :{param.Name}");
                    }

                    if (filterList.Any())
                    {
                        wraped = wraped + " WHERE " + filterList.Join(" AND ");
                    }

                    if (sort_order.Any())
                    {
                        List<string> sort = new List<string>();
                        sort_order.ForEach(item => sort.Add($"{item.Name} {item.GetString()}"));
                        wraped = wraped + " ORDER BY " + sort.Join($" {CharConstants.COMMA} ");
                    }
                }

                wraped = $"SELECT COUNT(*) FROM ({wraped}) AS COUNT_STAR;" + wraped;

                if (has_limit)
                    wraped += $" LIMIT :limit OFFSET :offset";

                if (!wraped.EndsWith(CharConstants.SEMI_COLON))
                    wraped += CharConstants.SEMI_COLON;
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return wraped;
        }

        public GetMobileFormDataResponse Get(GetMobileFormDataRequest request)
        {
            GetMobileFormDataResponse response = new GetMobileFormDataResponse();
            try
            {
                EbMobilePage mPage = this.Redis.Get<EbMobilePage>(request.MobilePageRefId);
                if (mPage == null)
                {
                    var obj = this.Gateway.Send<EbObjectParticularVersionResponse>(new EbObjectParticularVersionRequest
                    {
                        RefId = request.MobilePageRefId
                    });
                    mPage = EbSerializers.Json_Deserialize<EbMobilePage>(obj.Data[0].Json);
                }

                EbMobileForm formContainer = mPage.Container as EbMobileForm;
                if (!string.IsNullOrEmpty(formContainer.WebFormRefId))
                {
                    if (request.RowId != 0)
                    {
                        GetRowDataResponse row_resp = this.Gateway.Send<GetRowDataResponse>(new GetRowDataRequest
                        {
                            SolnId = request.SolnId,
                            UserId = request.UserId,
                            UserAuthId = request.UserAuthId,
                            RefId = formContainer.WebFormRefId,
                            RowId = request.RowId,
                            CurrentLoc = request.LocId,
                        });

                        if (!string.IsNullOrEmpty(row_resp.FormDataWrap))
                        {
                            WebformDataWrapper wraper = JsonConvert.DeserializeObject<WebformDataWrapper>(row_resp.FormDataWrap);
                            response.Data = wraper.FormData;
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
                Console.WriteLine(ex.StackTrace);
            }
            return response;
        }

        public GetMyActionsResponse Get(GetMyActionsRequest request)
        {
            GetMyActionsResponse response = new GetMyActionsResponse();
            try
            {
                User UserObject = GetUserObject(request.UserAuthId);

                DbParameter[] parameters = {
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("userid", EbDbTypes.String, request.UserId.ToString()),
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("roleids", EbDbTypes.String, UserObject.RoleIds.Join(",")),
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("usergroupids", EbDbTypes.String, UserObject.UserGroupIds.Join(","))
                };

                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(EbConnectionFactory.ObjectsDB.EB_GET_MYACTIONS, parameters);

                if (dt != null)
                {
                    foreach (EbDataRow row in dt.Rows)
                    {
                        EbMyActionsMobile action = new EbMyActionsMobile
                        {
                            Id = Convert.ToInt32(row["id"]),
                            StartDate = Convert.ToDateTime(row["from_datetime"]),
                            StageId = Convert.ToInt32(row["eb_stages_id"]),
                            WebFormRefId = row["form_ref_id"]?.ToString(),
                            WebFormDataId = Convert.ToInt32(row["form_data_id"]),
                            ApprovalLinesId = Convert.ToInt32(row["eb_approval_lines_id"]),
                            Description = row["description"]?.ToString()
                        };
                        response.Actions.Add(action);

                        try
                        {
                            action.ActionType = row["my_action_type"].ToString().ToEnum<MyActionTypes>();
                        }
                        catch
                        {
                            Console.WriteLine("Parse error");
                            Console.WriteLine("Failed to parse my_action_type to enum");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception at GetMyActionsRequest");
                Console.WriteLine(ex.Message);
            }
            return response;
        }

        public GetMyActionInfoResponse Get(GetMyActionInfoRequest request)
        {
            GetMyActionInfoResponse response = new GetMyActionInfoResponse();
            try
            {
                User UserObject = GetUserObject(request.UserAuthId);
                string query = @"SELECT 
	                                ES.stage_name,ES.stage_unique_id,ESA.action_unique_id,ESA.action_name
                                FROM 
	                                eb_stages ES
                                LEFT JOIN 
	                                eb_stage_actions ESA
                                ON 
	                                ESA.eb_stages_id = ES.id
                                WHERE 
	                                ES.id = :stageid
                                AND
	                                COALESCE(ES.eb_del, 'F') = 'F'
                                AND
	                                COALESCE(ESA.eb_del, 'F') = 'F';";

                List<DbParameter> parameters = new List<DbParameter> {
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("stageid", EbDbTypes.Int32, request.StageId)
                };

                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query, parameters.ToArray()) ?? new EbDataTable();

                WebFormServices webFormService = base.ResolveService<WebFormServices>();
                GetFormData4MobileResponse resp = webFormService.Any(new GetFormData4MobileRequest
                {
                    RefId = request.WebFormRefId,
                    DataId = request.WebFormDataId,
                    SolnId = request.SolnId,
                    UserAuthId = request.UserAuthId,
                    UserId = request.UserId
                });
                response.Data = resp.Params;

                if (dt.Rows.Any())
                {
                    var first = dt.Rows.First();

                    foreach (EbDataRow row in dt.Rows)
                    {
                        if (row == first)
                        {
                            response.StageUniqueId = first["stage_unique_id"].ToString();
                            response.StageName = first["stage_name"].ToString();
                        }

                        response.StageActions.Add(new EbStageActionsMobile
                        {
                            ActionName = row["action_name"].ToString(),
                            ActionUniqueId = row["action_unique_id"].ToString()
                        });
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine(ex.Message);
            }
            return response;
        }

        public EbDeviceRegistration Post(EbDeviceRegistrationRequest request)
        {
            EbDeviceRegistration registration = new EbDeviceRegistration();

            try
            {
                var actionResp = this.Get(new GetMyActionsRequest
                {
                    UserAuthId = request.UserAuthId,
                    UserId = request.UserId
                });
                registration.ActionsCount = actionResp.Actions.Count;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Get actions error inside eb device reg request");
                Console.WriteLine(ex.Message);
            }

            string query = @"SELECT * FROM eb_notifications WHERE user_id = :userid AND message_seen ='F' ORDER BY created_at DESC;";

            DbParameter[] parameters = {
                this.EbConnectionFactory.ObjectsDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
            };

            try
            {
                var dt = this.EbConnectionFactory.DataDB.DoQuery(query, parameters);

                if (dt != null)
                {

                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Get notification query error");
                Console.WriteLine(ex.Message);
            }

            return registration;
        }
    }
}
