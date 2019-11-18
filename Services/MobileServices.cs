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
                const string idcheck = "AND EOA.obj_id = ANY(string_to_array(:ids, ',')::int[])";
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
        public ObjectListToMob Get(ObjectListToMobRequest request)
        {
            Dictionary<int, List<ObjWrap>> dict = new Dictionary<int, List<ObjWrap>>();

            try
            {
                User UserObject = this.Redis.Get<User>(request.UserAuthId);
                string[] PermIds = UserObject.GetAccessIds(request.LocationId);

                string Sql = @"SELECT
                                   EO.id, EO.obj_type, EO.obj_name,
                                   EOV.version_num, 
                                   EOV.refid,
                                   display_name
                                FROM
   	                                eb_objects EO, eb_objects_ver EOV, eb_objects_status EOS, eb_objects2application EO2A 
                                WHERE
   	                                EOV.eb_objects_id = EO.id	
                                AND EO2A.app_id = :appid	      			    
                                AND EOS.eb_obj_ver_id = EOV.id 
                                AND EO2A.obj_id = EO.id
                                AND EO2A.eb_del = 'F'
                                AND EOS.status = 3 
                                AND COALESCE( EO.eb_del, 'F') = 'F'
                                AND EOS.id = ANY( Select MAX(id) from eb_objects_status EOS Where EOS.eb_obj_ver_id = EOV.id );";

                DbParameter[] parameters = {
                    this.EbConnectionFactory.ObjectsDB.GetNewParameter("appid",EbDbTypes.Int32,request.AppId)
                };

                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(Sql, parameters);

                foreach (EbDataRow dr in dt.Rows)
                {
                    int _ObjType = Convert.ToInt32(dr["obj_type"]);
                    EbObjectType _EbObjType = (EbObjectType)_ObjType;

                    string _ObjId = dr["id"].ToString();

                    if (!_EbObjType.IsUserFacing || !PermIds.Contains(_ObjId))
                        continue;

                    if (!dict.ContainsKey(_ObjType))
                        dict.Add(_ObjType, new List<ObjWrap>());

                    dict[_ObjType].Add(new ObjWrap
                    {
                        Id = Convert.ToInt32(dr["id"]),
                        EbObjectType = Convert.ToInt32(dr["obj_type"]),
                        Refid = dr["refid"].ToString(),
                        EbType = _EbObjType.Name,
                        DisplayName = dr["display_name"].ToString(),
                        VersionNumber = dr["version_num"].ToString()
                    });
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception at sidebar user mobile req ::" + ex.Message);
            }

            return new ObjectListToMob { ObjectTypes = dict };
        }

        public EbObjectToMobResponse Get(EbObjectToMobRequest request)
        {
            EbObjectToMobResponse response = new EbObjectToMobResponse();
            EbObjectService StudioServices = base.ResolveService<EbObjectService>();
            try
            {
                var resp = (EbObjectParticularVersionResponse)StudioServices.Get(new EbObjectParticularVersionRequest { RefId = request.RefId });

                if (!resp.Data.Any())
                    return null;

                response.ObjectWraper = resp.Data[0];

                if (resp.Data[0].EbObjectType == EbObjectTypes.Report)
                {

                }
                else if (resp.Data[0].EbObjectType == EbObjectTypes.TableVisualization)
                {

                }
            }
            catch (Exception e)
            {
                Console.WriteLine("EXCEPTION AT EbObjectToMobResponse" + e.Message);
                Console.WriteLine(e.StackTrace);
            }

            return response;
        }
    }
}
