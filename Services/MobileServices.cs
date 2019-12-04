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
using System.Threading.Tasks;
using ExpressBase.Security;
using ExpressBase.Common.Extensions;
using System.Data.Common;
using Newtonsoft.Json;
using ExpressBase.Objects.Objects.MobilePage;

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
                WebFormSchema schema = (request.MobilePage.Container as EbMobileForm).ToWebFormSchema();

                List<TableColumnMeta> _TableColMeta = this.GenerateTableColumnMeta(schema);

                WebFormServices webservices = base.ResolveService<WebFormServices>();

                int status = webservices.CreateOrAlterTable(schema.FormName, _TableColMeta, ref msg);

                //deploy webform if create 
                if (status == 0)
                {
                    this.DeployWebForm(request);
                }

                if ((request.MobilePage.Container as EbMobileForm).AutoDeployMV)
                    this.DeployMobileVis(request, _TableColMeta);
            }
            catch (Exception e)
            {
                Console.WriteLine("EXCEPTION AT MOBILE TABLE CREATION : " + e.Message);
                Console.WriteLine(e.StackTrace);
            }
            return response;
        }

        private List<TableColumnMeta> GenerateTableColumnMeta(WebFormSchema _webschema)
        {
            IVendorDbTypes vDbTypes = this.EbConnectionFactory.DataDB.VendorDbTypes;
            List<TableColumnMeta> _metaList = new List<TableColumnMeta>();
            foreach (TableSchema _table in _webschema.Tables)
            {
                if (_table.Columns.Count > 0)
                {
                    foreach (ColumnSchema _column in _table.Columns)
                    {
                        _metaList.Add(new TableColumnMeta
                        {
                            Name = _column.ColumnName,
                            Type = vDbTypes.GetVendorDbTypeStruct((EbDbTypes)_column.EbDbType)
                        });
                    }

                    _metaList.Add(new TableColumnMeta { Name = "eb_created_by", Type = vDbTypes.Decimal, Label = "Created By" });
                    _metaList.Add(new TableColumnMeta { Name = "eb_created_at", Type = vDbTypes.DateTime, Label = "Created At" });
                    _metaList.Add(new TableColumnMeta { Name = "eb_lastmodified_by", Type = vDbTypes.Decimal, Label = "Last Modified By" });
                    _metaList.Add(new TableColumnMeta { Name = "eb_lastmodified_at", Type = vDbTypes.DateTime, Label = "Last Modified At" });
                    _metaList.Add(new TableColumnMeta { Name = "eb_del", Type = vDbTypes.Boolean, Default = "F" });
                    _metaList.Add(new TableColumnMeta { Name = "eb_void", Type = vDbTypes.Boolean, Default = "F", Label = "Void ?" });
                    _metaList.Add(new TableColumnMeta { Name = "eb_loc_id", Type = vDbTypes.Int32, Label = "Location" });
                    _metaList.Add(new TableColumnMeta { Name = "eb_device_id", Type = vDbTypes.String, Label = "Device Id" });
                    _metaList.Add(new TableColumnMeta { Name = "eb_appversion", Type = vDbTypes.String, Label = "App Version" });
                    _metaList.Add(new TableColumnMeta { Name = "eb_created_at_device", Type = vDbTypes.DateTime, Label = "Sync Time" });
                }
            }
            return _metaList;
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
                    Name = request.MobilePage.Name + "_AutoGenVis",
                    DisplayName = request.MobilePage.DisplayName + "_AutoGenVis"
                };

                EbMobileVisualization _vis = new EbMobileVisualization
                {
                    Name = "tab0_visualization_autogen",
                    DataSourceRefId = this.CreateDataReader(request, cols)
                };

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
                    foreach (EbMobileControl ctrl in mobileform.ChiledControls)
                    {
                        string name = ctrl.GetType().Name;

                        if (counter.ContainsKey(name))
                            counter[name]++;
                        else
                            counter[name] = 0;

                        EbControl Wctrl = ctrl.GetWebFormCtrl(counter[name]);
                        webform.Controls.Add(Wctrl);
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

                string idcheck = EbConnectionFactory.DataDB.EB_GET_MOBILE_PAGES;

                const string Sql = @"SELECT obj_name,display_name,version_num,obj_json FROM (
				                                SELECT 
					                                EO.id,EO.obj_name,EO.display_name,EOV.version_num, EOV.obj_json
				                                FROM
					                                eb_objects EO
				                                LEFT JOIN 
					                                eb_objects_ver EOV ON (EOV.eb_objects_id = EO.id)
				                                LEFT JOIN
					                                eb_objects_status EOS ON (EOS.eb_obj_ver_id = EOV.id)
				                                WHERE
					                                COALESCE(EO.eb_del, 'F') = 'F'
				                                AND
					                                EOS.status = 3
				                                AND 
					                                EO.obj_type = 13
				                                AND 
					                                EOS.id = ANY( Select MAX(id) from eb_objects_status EOS Where EOS.eb_obj_ver_id = EOV.id)
				                                ) OD 
                                LEFT JOIN eb_objects2application EO2A ON (EO2A.obj_id = OD.id)
                                WHERE 
	                                EO2A.app_id = :appid 
                                {0}
                                AND 
	                                COALESCE(EO2A.eb_del, 'F') = 'F';";

                List<DbParameter> parameters = new List<DbParameter> {
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("appid", EbDbTypes.Int32, request.AppId)
                };

                if (UserObject.Roles.Contains(SystemRoles.SolutionOwner.ToString()) || UserObject.Roles.Contains(SystemRoles.SolutionAdmin.ToString()))
                {
                    query = string.Format(Sql, string.Empty);
                }
                else
                {
                    parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("objids", EbDbTypes.String, string.Join(",", PermIds)));
                    query = string.Format(Sql, idcheck);
                }

                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query, parameters.ToArray());

                foreach (EbDataRow dr in dt.Rows)
                {
                    response.Pages.Add(new MobilePagesWraper
                    {
                        Name = dr["obj_name"].ToString(),
                        DisplayName = dr["display_name"].ToString(),
                        Version = dr["version_num"].ToString(),
                        Json = dr["obj_json"].ToString()
                    });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception at object list for user mobile req ::" + ex.Message);
            }

            return response;
        }
    }
}
