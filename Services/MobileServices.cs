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

namespace ExpressBase.ServiceStack.Services
{
    public class MobileServices : EbBaseService
    {
        public MobileServices(IEbConnectionFactory _dbf, IMessageProducer _mqp) : base(_dbf, _mqp) { }

        public CreateMobileFormTableResponse Post(CreateMobileFormTableRequest request)
        {
            CreateMobileFormTableResponse response = new CreateMobileFormTableResponse();
            string msg = string.Empty;
            try
            {
                EbMobileForm mobileForm = (request.MobilePage.Container as EbMobileForm);
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
                    EbMobileForm mobileform = (request.MobilePage.Container as EbMobileForm);

                    EbWebForm webform = new EbWebForm
                    {
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

        //get applist for mobile
        public GetMobMenuResonse Get(GetMobMenuRequest request)
        {
            GetMobMenuResonse resp = new GetMobMenuResonse();
            try
            {
                string sql = string.Empty;
                EbDataTable dt = null;
                string idcheck = EbConnectionFactory.DataDB.EB_GET_MOB_MENU_OBJ_IDS;
                const string acquery = @"SELECT 
	                                        EA.id,
	                                        EA.applicationname,
	                                        EA.app_icon,
	                                        EA.application_type 
                                        FROM 
	                                        eb_applications EA
                                        WHERE
	                                        EXISTS
                                                (
                                                SELECT *
                                                FROM eb_objects2application EOA
                                                WHERE EOA.app_id = EA.id AND EOA.eb_del = 'F' {0}
                                                )
                                        AND
	                                        EA.eb_del = 'F'
                                        AND
	                                        EA.application_type = 2;";

                User UserObject = this.Redis.Get<User>(request.UserAuthId);
                string[] Ids = UserObject.GetAccessIds(request.LocationId);

                if (UserObject.Roles.Contains(SystemRoles.SolutionOwner.ToString()) || UserObject.Roles.Contains(SystemRoles.SolutionAdmin.ToString()))
                {
                    sql = string.Format(acquery, string.Empty);
                    dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql);
                }
                else
                {
                    sql = string.Format(acquery, idcheck);

                    DbParameter[] parameters =
                    {
                        this.EbConnectionFactory.DataDB.GetNewParameter("ids", EbDbTypes.String, String.Join(",",Ids)),
                    };
                    dt = this.EbConnectionFactory.ObjectsDB.DoQuery(sql, parameters);
                }

                foreach (EbDataRow row in dt.Rows)
                {
                    resp.Applications.Add(new AppDataToMob
                    {
                        AppId = Convert.ToInt32(row["id"]),
                        AppName = row["applicationname"].ToString(),
                        AppIcon = row["app_icon"].ToString(),
                    });
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace);
                Console.WriteLine("EXCEPTION AT GetMobMenuRequest " + e.Message);
            }
            return resp;
        }

        //objectlist for mobile
        public GetMobilePagesResponse Get(GetMobilePagesRequest request)
        {
            GetMobilePagesResponse response = new GetMobilePagesResponse();
            try
            {
                User UserObject = this.Redis.Get<User>(request.UserAuthId);
                string[] PermIds = UserObject.GetAccessIds(request.LocationId);
                string query = string.Empty;
                string idcheck = EbConnectionFactory.ObjectsDB.EB_GET_MOBILE_PAGES;
                string Sql = EbConnectionFactory.ObjectsDB.EB_GET_MOBILE_PAGES_OBJS;

                List<DbParameter> parameters = new List<DbParameter> {
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("appid", EbDbTypes.Int32, request.AppId)
                };

                if (UserObject.Roles.Contains(SystemRoles.SolutionOwner.ToString()) || UserObject.Roles.Contains(SystemRoles.SolutionAdmin.ToString()))
                    query = string.Format(Sql, string.Empty);
                else
                {
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("objids", EbDbTypes.String, PermIds.Join(",")));
                    query = string.Format(Sql, idcheck);
                }
                EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(query, parameters.ToArray());
                foreach (EbDataRow dr in ds.Tables[0].Rows)
                {
                    EbObjectType objType = (EbObjectType)Convert.ToInt32(dr["obj_type"]);
                    if (objType.IntCode == EbObjectTypes.MobilePage)
                    {
                        response.Pages.Add(new MobilePagesWraper
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
                        response.WebObjects.Add(new WebObjectsWraper
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
                //apps settings
                if (ds.Tables.Count >= 2 && ds.Tables[1].Rows.Any())
                {
                    EbMobileSettings settings = JsonConvert.DeserializeObject<EbMobileSettings>(ds.Tables[1].Rows[0]["app_settings"].ToString());
                    if (settings != null)
                    {
                        response.TableNames = settings.DataImport.Select(i => i.TableName).ToList();
                        if (request.PullData)
                            response.Data = this.PullAppConfiguredData(settings);
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception at object list for user mobile req ::" + ex.Message);
                Console.WriteLine(ex.StackTrace);
                return response;
            }
            return response;
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

                        resp.DataSet.Tables[0].TableName = DI.TableName;
                        DataSet.Tables.Add(resp.DataSet.Tables[0]);
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
                    var obj = this.Gateway.Send<EbObjectParticularVersionResponse>(new EbObjectParticularVersionRequest
                    {
                        RefId = request.DataSourceRefId
                    });
                    _ds = EbSerializers.Json_Deserialize<EbDataReader>(obj.Data[0].Json);
                }

                List<DbParameter> parameters = request.Params.ParamsToDbParameters(this.EbConnectionFactory);

                if (request.Limit != 0)
                {
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@limit", EbDbTypes.Int32, request.Limit));
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("@offset", EbDbTypes.Int32, request.Offset));
                }
                string wraped = WrapQuery(_ds.Sql, request.Limit, request.Offset);

                resp.Data = this.EbConnectionFactory.DataDB.DoQueries(wraped, parameters.ToArray());
            }
            catch (Exception ex)
            {
                resp.Message = "No Data";
                Console.WriteLine("Exception at object list for user mobile req ::" + ex.Message);
            }
            return resp;
        }

        private string WrapQuery(string sql, int limit, int offset)
        {
            string wraped = $"SELECT COUNT(*) FROM ({sql.TrimEnd(CharConstants.SEMI_COLON)}) AS COUNT_STAR;";
            if (limit != 0)
            {
                string[] queries = sql.Split(CharConstants.SEMI_COLON);

                for (int i = 0; i < queries.Length; i++)
                    wraped += $"SELECT * FROM ({queries[i]}) AS WRPR{i} LIMIT @limit OFFSET @offset;";
            }
            else
                wraped += sql;

            if (!wraped.EndsWith(CharConstants.SEMI_COLON))
                wraped += CharConstants.SEMI_COLON;

            return wraped;
        }

        public GetMyActionsResponse Get(GetMyActionsRequest request)
        {
            GetMyActionsResponse response = new GetMyActionsResponse();
            try
            {
                User UserObject = this.Redis.Get<User>(request.UserAuthId);

                string query = EbConnectionFactory.ObjectsDB.EB_GET_MYACTIONS;

                List<DbParameter> parameters = new List<DbParameter> {
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("userid", EbDbTypes.Int32, request.UserId),
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("roleids", EbDbTypes.String, UserObject.RoleIds.Join(",")),
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("usergroupids", EbDbTypes.String, UserObject.UserGroupIds.Join(","))
                };

                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query, parameters.ToArray()) ?? new EbDataTable();

                foreach (EbDataRow row in dt.Rows)
                {
                    response.Actions.Add(new EbMyActionsMobile
                    {
                        Id = Convert.ToInt32(row["id"]),
                        StartDate = Convert.ToDateTime(row["from_datetime"]),
                        EndDate = Convert.ToDateTime(row["completed_at"]),
                        StageId = Convert.ToInt32(row["eb_stages_id"]),
                        WebFormRefId = row["form_ref_id"].ToString(),
                        WebFormDataId = Convert.ToInt32(row["form_data_id"]),
                        ApprovalLinesId = Convert.ToInt32(row["eb_approval_lines_id"]),
                        Description = row["description"].ToString()
                    });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception at GetMyActionsRequest ::" + ex.Message);
            }
            return response;
        }

        public GetMyActionInfoResponse Get(GetMyActionInfoRequest request)
        {
            GetMyActionInfoResponse response = new GetMyActionInfoResponse();
            try
            {
                User UserObject = this.Redis.Get<User>(request.UserAuthId);
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
                    UserObj = UserObject,
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
    }
}
