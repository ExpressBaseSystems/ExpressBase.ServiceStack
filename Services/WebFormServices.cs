using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.LocationNSolution;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Security;
using ExpressBase.Objects;
using ExpressBase.Objects.Objects;
using ExpressBase.Objects.Objects.DVRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.Objects.WebFormRelated;
using Jurassic;
using Jurassic.Library;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using Newtonsoft.Json;
using ServiceStack;
using ServiceStack.Messaging;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace ExpressBase.ServiceStack.Services
{
    [Authenticate]
    public class WebFormServices : EbBaseService
    {
        public WebFormServices(IEbConnectionFactory _dbf, IMessageProducer _mqp) : base(_dbf, _mqp) { }

        //========================================== FORM TABLE CREATION  ==========================================

        public CreateWebFormTableResponse Any(CreateWebFormTableRequest request)
        {
            if (request.WebObj is EbWebForm)
            {
                EbWebForm Form = request.WebObj as EbWebForm;
                Form.AfterRedisGet(this);
                if (Form.ExeDataPusher)
                {
                    foreach (EbDataPusher pusher in Form.DataPushers)
                    {
                        EbWebForm _form = this.GetWebFormObject(pusher.FormRefId, null, null);
                        TableSchema _table = _form.FormSchema.Tables.Find(e => e.TableName.Equals(_form.FormSchema.MasterTable));
                        //_table.Columns.Add(new ColumnSchema { ColumnName = "eb_push_id", EbDbType = (int)EbDbTypes.String, Control = new EbTextBox { Name = "eb_push_id", Label = "Push Id" } });// multi push id
                        //_table.Columns.Add(new ColumnSchema { ColumnName = "eb_src_id", EbDbType = (int)EbDbTypes.Decimal, Control = new EbNumeric { Name = "eb_src_id", Label = "Source Id" } });// source master table id
                        if (_table != null)
                            Form.FormSchema.Tables.Add(_table);
                    }
                }
                CreateWebFormTables(Form.FormSchema, request);
                InsertDataIfRequired(Form.FormSchema, Form.RefId);
            }
            return new CreateWebFormTableResponse { };
        }

        public CreateMyProfileTableResponse Any(CreateMyProfileTableRequest request)
        {
            foreach (EbProfileUserType eput in request.UserTypeForms)
            {
                if (eput.RefId != string.Empty)
                {
                    EbWebForm form = this.GetWebFormObject(eput.RefId, null, null);
                    TableSchema _table = form.FormSchema.Tables.Find(e => e.TableName.Equals(form.FormSchema.MasterTable));
                    if (_table != null)
                    {
                        form.AutoDeployTV = false;
                        _table.Columns.Add(new ColumnSchema { ColumnName = "eb_users_id", EbDbType = (int)EbDbTypes.Int32, Control = new EbNumeric { Name = "eb_users_id", Label = "User Id" } });
                        CreateWebFormTables(form.FormSchema, new CreateWebFormTableRequest { WebObj = form, DontThrowException = true });
                    }
                }
            }
            return new CreateMyProfileTableResponse { };
        }

        //Review control related data
        private void InsertDataIfRequired(WebFormSchema _schema, string _refId)
        {
            EbReview reviewCtrl = (EbReview)_schema.ExtendedControls.Find(e => e is EbReview);
            if (reviewCtrl == null || _refId == null)
                return;
            int[] stageIds = new int[reviewCtrl.FormStages.Count];
            string selQ = @"SELECT id FROM eb_stages WHERE form_ref_id = @form_ref_id AND COALESCE(eb_del, 'F') = 'F' ORDER BY id; ";
            EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(selQ, new DbParameter[] { this.EbConnectionFactory.DataDB.GetNewParameter("form_ref_id", EbDbTypes.String, _refId) });
            for (int i = 0; i < dt.Rows.Count && i < stageIds.Length; i++)
            {
                int.TryParse(dt.Rows[i][0].ToString(), out stageIds[i]);
            }
            string fullQ = $@"UPDATE eb_stage_actions SET eb_del = 'T' WHERE COALESCE(eb_del, 'F') = 'F' AND eb_stages_id IN (SELECT id FROM eb_stages WHERE  form_ref_id = @form_ref_id AND COALESCE(eb_del, 'F') = 'F');
                            UPDATE eb_stages SET eb_del = 'T' WHERE form_ref_id = @form_ref_id AND COALESCE(eb_del, 'F') = 'F' AND id NOT IN ({stageIds.Join(",")}); ";

            List<DbParameter> param = new List<DbParameter>();
            param.Add(this.EbConnectionFactory.DataDB.GetNewParameter("form_ref_id", EbDbTypes.String, _refId));
            for (int i = 0; i < stageIds.Length; i++)
            {
                EbReviewStage reviewStage = reviewCtrl.FormStages[i] as EbReviewStage;
                string stageid = "(SELECT eb_currval('eb_stages_id_seq'))";
                if (stageIds[i] == 0)
                {
                    fullQ += $@"INSERT INTO eb_stages(stage_name, stage_unique_id, form_ref_id, eb_del) 
                                VALUES (@stage_name_{i}, @stage_unique_id_{i}, @form_ref_id, 'F'); ";
                    if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.MYSQL)
                        fullQ += $"SELECT eb_persist_currval('eb_stages_id_seq'); ";
                }
                else
                {
                    fullQ += $@"UPDATE eb_stages SET stage_name = @stage_name_{i}, stage_unique_id = @stage_unique_id_{i}
                                    WHERE form_ref_id = @form_ref_id AND id = {stageIds[i]}; ";
                    stageid = stageIds[i].ToString();
                }

                param.Add(this.EbConnectionFactory.DataDB.GetNewParameter($"stage_name_{i}", EbDbTypes.String, reviewStage.Name));
                param.Add(this.EbConnectionFactory.DataDB.GetNewParameter($"stage_unique_id_{i}", EbDbTypes.String, reviewStage.EbSid));

                for (int j = 0; j < reviewStage.StageActions.Count; j++)
                {
                    EbReviewAction reviewAction = reviewStage.StageActions[j] as EbReviewAction;
                    fullQ += $@"INSERT INTO eb_stage_actions(action_name, action_unique_id, eb_stages_id, eb_del) 
                                    VALUES (@action_name_{i}_{j}, @action_unique_id_{i}_{j}, {stageid}, 'F'); ";
                    param.Add(this.EbConnectionFactory.DataDB.GetNewParameter($"action_name_{i}_{j}", EbDbTypes.String, reviewAction.Name));
                    param.Add(this.EbConnectionFactory.DataDB.GetNewParameter($"action_unique_id_{i}_{j}", EbDbTypes.String, reviewAction.EbSid));
                }
            }

            this.EbConnectionFactory.DataDB.DoNonQuery(fullQ, param.ToArray());

        }

        private void CreateWebFormTables(WebFormSchema _schema, CreateWebFormTableRequest request)
        {
            IVendorDbTypes vDbTypes = this.EbConnectionFactory.DataDB.VendorDbTypes;
            string Msg = string.Empty;
            foreach (TableSchema _table in _schema.Tables)
            {
                List<TableColumnMeta> _listNamesAndTypes = new List<TableColumnMeta>();
                if (_table.Columns.Count > 0 && _table.TableType != WebFormTableTypes.Review)
                {
                    foreach (ColumnSchema _column in _table.Columns)
                    {
                        if (_column.Control is EbAutoId)
                        {
                            _listNamesAndTypes.Add(new TableColumnMeta { Name = _column.ColumnName, Type = vDbTypes.GetVendorDbTypeStruct((EbDbTypes)_column.EbDbType), Unique = true, Control = _column.Control, Label = _column.Control.Label });
                            _listNamesAndTypes.Add(new TableColumnMeta { Name = _column.ColumnName + "_ebbkup", Type = vDbTypes.GetVendorDbTypeStruct((EbDbTypes)_column.EbDbType), Label = _column.Control.Label + "_ebbkup" });
                        }
                        else if (_column.Control.DoNotPersist || _column.Control.IsSysControl)
                            continue;
                        else
                            _listNamesAndTypes.Add(new TableColumnMeta { Name = _column.ColumnName, Type = vDbTypes.GetVendorDbTypeStruct((EbDbTypes)_column.EbDbType), Label = _column.Control.Label, Control = _column.Control });
                    }
                    if (_table.TableName == _schema.MasterTable)
                    {
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_ver_id", Type = vDbTypes.Decimal });// id refernce to the parent table will store in this column - foreignkey
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lock", Type = vDbTypes.Boolean, Default = "F", Label = "Lock ?" });// lock to prevent editing
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_push_id", Type = vDbTypes.String, Label = "Multi push id" });// multi push id - for data pushers
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_src_id", Type = vDbTypes.Decimal, Label = "Source id" });// source id - for data pushers
                    }
                    else
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = _schema.MasterTable + "_id", Type = vDbTypes.Decimal });// id refernce to the parent table will store in this column - foreignkey
                    if (_table.TableType == WebFormTableTypes.Grid)
                    {
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_row_num", Type = vDbTypes.Decimal });// data grid row number
                        if (_table.IsDynamic)// if data grid is in dynamic tab then adding column for source reference - foreignkey
                        {
                            foreach (TableSchema _t in _schema.Tables.FindAll(e => e.TableType == WebFormTableTypes.Grid && e != _table))
                                _listNamesAndTypes.Add(new TableColumnMeta { Name = _t.TableName + "_id", Type = vDbTypes.Decimal });
                        }
                    }

                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_by", Type = vDbTypes.Decimal, Label = "Created By" });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_at", Type = vDbTypes.DateTime, Label = "Created At" });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_by", Type = vDbTypes.Decimal, Label = "Last Modified By" });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_at", Type = vDbTypes.DateTime, Label = "Last Modified At" });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_del", Type = vDbTypes.Boolean, Default = "F" });// delete
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_void", Type = vDbTypes.Boolean, Default = "F", Label = "Void ?" });// cancel //only ?
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_loc_id", Type = vDbTypes.Int32, Label = "Location" });// location id //only ?
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_signin_log_id", Type = vDbTypes.Int32, Label = "Log Id" });
                    //_listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_default", Type = vDbTypes.Boolean, Default = "F" });

                    int _rowaff = CreateOrAlterTable(_table.TableName, _listNamesAndTypes, ref Msg);
                    if (_table.TableName == _schema.MasterTable && !request.IsImport && (request.WebObj as EbWebForm).AutoDeployTV)
                    {
                        if (_schema.ExtendedControls.Find(e => e is EbReview) != null)
                            _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_approval", Label = "Approval" });
                        CreateOrUpdateDsAndDv(request, _listNamesAndTypes);
                    }
                }
            }

            if (!request.DontThrowException && !Msg.IsEmpty())
                throw new FormException(Msg);
        }

        public int CreateOrAlterTable(string tableName, List<TableColumnMeta> listNamesAndTypes, ref string Msg)
        {
            int status = -1;

            //checking for space in column name, table name
            if (tableName.Contains(CharConstants.SPACE))
                throw new FormException("Table creation failed - Invalid table name: " + tableName);
            foreach (TableColumnMeta entry in listNamesAndTypes)
                if (entry.Name.Contains(CharConstants.SPACE))
                    throw new FormException("Table creation failed : Invalid column name" + entry.Name);

            var isTableExists = this.EbConnectionFactory.DataDB.IsTableExists(this.EbConnectionFactory.DataDB.IS_TABLE_EXIST, new DbParameter[] { this.EbConnectionFactory.DataDB.GetNewParameter("tbl", EbDbTypes.String, tableName) });
            if (!isTableExists)
            {
                string cols = string.Join(CharConstants.COMMA + CharConstants.SPACE.ToString(), listNamesAndTypes.Select(x => x.Name + CharConstants.SPACE + x.Type.VDbType.ToString() + (x.Default.IsNullOrEmpty() ? "" : (" DEFAULT '" + x.Default + "'"))).ToArray());
                string sql = string.Empty;
                if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.ORACLE)////////////
                {
                    sql = "CREATE TABLE @tbl(id NUMBER(10), @cols)".Replace("@cols", cols).Replace("@tbl", tableName);
                    this.EbConnectionFactory.DataDB.CreateTable(sql);//Table Creation
                    CreateSquenceAndTrigger(tableName);
                }
                else if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.PGSQL)
                {
                    sql = "CREATE TABLE @tbl( id SERIAL PRIMARY KEY, @cols)".Replace("@cols", cols).Replace("@tbl", tableName);
                    this.EbConnectionFactory.DataDB.CreateTable(sql);
                }
                else if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.MYSQL)
                {
                    sql = "CREATE TABLE @tbl( id INTEGER AUTO_INCREMENT PRIMARY KEY, @cols)".Replace("@cols", cols).Replace("@tbl", tableName);
                    this.EbConnectionFactory.DataDB.CreateTable(sql);
                }
                status = 0;
            }
            else
            {
                var colSchema = this.EbConnectionFactory.DataDB.GetColumnSchema(tableName);
                string sql = string.Empty;
                foreach (TableColumnMeta entry in listNamesAndTypes)
                {
                    bool isFound = false;
                    foreach (EbDataColumn dr in colSchema)
                    {
                        if (entry.Name.ToLower() == (dr.ColumnName.ToLower()))
                        {
                            if (entry.Type.EbDbType != dr.Type && !(
                                (entry.Type.EbDbType.ToString().Equals("Boolean") && dr.Type.ToString().Equals("String")) ||
                                (entry.Type.EbDbType.ToString().Equals("BooleanOriginal") && dr.Type.ToString().Equals("Boolean")) ||
                                (entry.Type.EbDbType.ToString().Equals("Decimal") && (dr.Type.ToString().Equals("Int32") || dr.Type.ToString().Equals("Int64"))) ||
                                (entry.Type.EbDbType.ToString().Equals("DateTime") && dr.Type.ToString().Equals("Date")) ||
                                (entry.Type.EbDbType.ToString().Equals("Date") && dr.Type.ToString().Equals("DateTime")) ||
                                (entry.Type.EbDbType.ToString().Equals("Time") && dr.Type.ToString().Equals("DateTime"))
                                ))
                                Msg += $"Type mismatch found '{dr.Type.ToString()}' instead of '{entry.Type.EbDbType}' for {tableName}.{entry.Name}; ";
                            //Msg += string.Format("Already exists '{0}' Column for {1}.{2}({3}); ", dr.Type.ToString(), tableName, entry.Name, entry.Type.EbDbType);                           
                            isFound = true;
                            break;
                        }
                    }
                    if (!isFound)
                    {
                        sql += entry.Name + " " + entry.Type.VDbType.ToString() + " " + (entry.Default.IsNullOrEmpty() ? "" : (" DEFAULT '" + entry.Default + "'")) + ",";
                    }
                }
                bool appendId = false;
                var existingIdCol = colSchema.FirstOrDefault(o => o.ColumnName.ToLower() == "id");
                if (existingIdCol == null)
                    appendId = true;
                if (!sql.IsEmpty() || appendId)
                {
                    if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.ORACLE)/////////////////////////
                    {
                        sql = (appendId ? "id NUMBER(10)," : "") + sql;
                        if (!sql.IsEmpty())
                        {
                            sql = "ALTER TABLE @tbl ADD (" + sql.Substring(0, sql.Length - 1) + ")";
                            sql = sql.Replace("@tbl", tableName);
                            int _aff = this.EbConnectionFactory.DataDB.UpdateTable(sql);
                            if (appendId)
                                CreateSquenceAndTrigger(tableName);
                        }
                    }
                    else if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.PGSQL)
                    {
                        sql = (appendId ? "id SERIAL PRIMARY KEY," : "") + sql;
                        if (!sql.IsEmpty())
                        {
                            sql = "ALTER TABLE @tbl ADD COLUMN " + (sql.Substring(0, sql.Length - 1)).Replace(",", ", ADD COLUMN ");
                            sql = sql.Replace("@tbl", tableName);
                            this.EbConnectionFactory.DataDB.UpdateTable(sql);
                        }
                    }
                    else if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.MYSQL)
                    {
                        sql = (appendId ? "id INTEGER AUTO_INCREMENT PRIMARY KEY," : "") + sql;
                        if (!sql.IsEmpty())
                        {
                            sql = "ALTER TABLE @tbl ADD COLUMN " + (sql.Substring(0, sql.Length - 1)).Replace(",", ", ADD COLUMN ");
                            sql = sql.Replace("@tbl", tableName);
                            this.EbConnectionFactory.DataDB.UpdateTable(sql);
                        }
                    }
                    status = 1;
                }
            }
            return status;
            //throw new FormException("Table creation failed - Table name: " + tableName);
        }

        private void CreateSquenceAndTrigger(string tableName)
        {
            string sqnceSql = "CREATE SEQUENCE @name_sequence".Replace("@name", tableName);
            string trgrSql = string.Format(@"CREATE OR REPLACE TRIGGER {0}_on_insert
													BEFORE INSERT ON {0}
													FOR EACH ROW
													BEGIN
														SELECT {0}_sequence.nextval INTO :new.id FROM dual;
													END;", tableName);
            this.EbConnectionFactory.DataDB.CreateTable(sqnceSql);//Sequence Creation
            this.EbConnectionFactory.DataDB.CreateTable(trgrSql);//Trigger Creation
        }

        private void CreateOrUpdateDsAndDv(CreateWebFormTableRequest request, List<TableColumnMeta> listNamesAndTypes)
        {
            IEnumerable<TableColumnMeta> _list = listNamesAndTypes.Where(x => x.Name != "eb_del" && x.Name != "eb_ver_id" && !(x.Name.Contains("_ebbkup")) && x.Name != "eb_push_id" && x.Name != "eb_src_id" && x.Name != "eb_lock" && x.Name != "eb_signin_log_id" && !(x.Control is EbFileUploader) && x.Name != "eb_approval");
            string cols = string.Join(CharConstants.COMMA + "\n \t ", _list.Select(x => x.Name).ToArray());
            EbTableVisualization dv = null;
            string AutogenId = (request.WebObj as EbWebForm).AutoGeneratedVizRefId;
            if (AutogenId.IsNullOrEmpty())
            {
                var dsid = CreateDataReader(request, cols);
                var dvrefid = CreateDataDataVisualization(request, listNamesAndTypes, dsid);
                (request.WebObj as EbWebForm).AutoGeneratedVizRefId = dvrefid;
                SaveFormObject(request);
            }
            else
            {
                dv = Redis.Get<EbTableVisualization>(AutogenId);
                if (dv == null)
                {
                    var result = this.Gateway.Send<EbObjectParticularVersionResponse>(new EbObjectParticularVersionRequest { RefId = AutogenId });
                    dv = EbSerializers.Json_Deserialize(result.Data[0].Json);
                    Redis.Set<EbTableVisualization>(AutogenId, dv);
                }
                UpdateDataReader(request, cols, dv, AutogenId);
                UpdateDataVisualization(request, listNamesAndTypes, dv, AutogenId);
            }
        }

        private string CreateDataReader(CreateWebFormTableRequest request, string cols)
        {
            EbDataReader drObj = new EbDataReader();
            drObj.Sql = "SELECT \n \t id,@colname@ FROM @tbl \n WHERE eb_del='F'".Replace("@tbl", request.WebObj.TableName).Replace("@colname@", cols);
            drObj.FilterDialogRefId = "";
            drObj.Name = request.WebObj.Name + "_AutoGenDR";
            drObj.DisplayName = request.WebObj.DisplayName + "_AutoGenDR";
            drObj.Description = request.WebObj.Description;
            return CreateNewObjectRequest(request, drObj);
        }

        private string CreateDataDataVisualization(CreateWebFormTableRequest request, List<TableColumnMeta> listNamesAndTypes, string dsid)
        {
            DVColumnCollection columns = GetDVColumnCollection(listNamesAndTypes, request);
            var dvobj = new EbTableVisualization();
            dvobj.Name = request.WebObj.Name + "_AutoGenDV";
            dvobj.DisplayName = request.WebObj.DisplayName + " List";
            dvobj.Description = request.WebObj.Description;
            dvobj.DataSourceRefId = dsid;
            dvobj.Columns = columns;
            dvobj.DSColumns = columns;
            dvobj.ColumnsCollection.Add(columns);
            dvobj.NotVisibleColumns = columns.FindAll(x => !x.bVisible);
            dvobj.AutoGen = true;
            dvobj.OrderBy = new List<DVBaseColumn>();
            dvobj.RowGroupCollection = new List<RowGroupParent>();
            dvobj.OrderBy.Add(columns.Get("eb_created_at"));
            SingleLevelRowGroup _rowgroup = new SingleLevelRowGroup();
            _rowgroup.DisplayName = "By Location";
            _rowgroup.Name = "groupbylocation";
            _rowgroup.RowGrouping.Add(columns.Get("eb_loc_id"));

            dvobj.RowGroupCollection.Add(_rowgroup);
            _rowgroup = new SingleLevelRowGroup();
            _rowgroup.DisplayName = "By Created By";
            _rowgroup.Name = "groupbycreatedby";
            _rowgroup.RowGrouping.Add(columns.Get("eb_created_by"));
            dvobj.RowGroupCollection.Add(_rowgroup);
            dvobj.BeforeSave(this, Redis);
            return CreateNewObjectRequest(request, dvobj);
        }

        private string CreateNewObjectRequest(CreateWebFormTableRequest request, EbObject dvobj)
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

        private void UpdateDataReader(CreateWebFormTableRequest request, string cols, EbTableVisualization dv, string AutogenId)
        {
            dv.AfterRedisGet(Redis, this);
            EbDataReader drObj = dv.EbDataSource;
            drObj.Sql = "SELECT \n \t id,@colname@ FROM @tbl \n WHERE eb_del='F'".Replace("@tbl", request.WebObj.TableName).Replace("@colname@", cols);
            drObj.FilterDialogRefId = "";
            drObj.Name = request.WebObj.Name + "_AutoGenDR";
            drObj.DisplayName = request.WebObj.DisplayName + "_AutoGenDR";
            drObj.Description = request.WebObj.Description;
            SaveObjectRequest(request, drObj);
        }

        private void UpdateDataVisualization(CreateWebFormTableRequest request, List<TableColumnMeta> listNamesAndTypes, EbTableVisualization dvobj, string AutogenId)
        {
            DVColumnCollection columns = UpdateDVColumnCollection(listNamesAndTypes, request, dvobj);
            dvobj.Name = request.WebObj.Name + "_AutoGenDV";
            dvobj.DisplayName = request.WebObj.DisplayName + " List";
            dvobj.Description = request.WebObj.Description;
            dvobj.Columns = columns;
            dvobj.DSColumns = columns;
            dvobj.ColumnsCollection[0] = columns;
            dvobj.NotVisibleColumns = columns.FindAll(x => !x.bVisible);
            UpdateOrderByObject(ref dvobj);
            UpdateRowGroupObject(ref dvobj);
            UpdateInfowindowObject(ref dvobj);
            //notchecked for formlink, treeview, customcolumn
            dvobj.BeforeSave(this, Redis);
            SaveObjectRequest(request, dvobj);
        }

        private void SaveFormObject(CreateWebFormTableRequest request)
        {
            EbWebForm obj = request.WebObj as EbWebForm;
            obj.BeforeSave(this);
            SaveObjectRequest(request, obj);
        }

        private void SaveObjectRequest(CreateWebFormTableRequest request, EbObject obj)
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

        private DVColumnCollection GetDVColumnCollection(List<TableColumnMeta> listNamesAndTypes, CreateWebFormTableRequest request)
        {
            IVendorDbTypes vDbTypes = this.EbConnectionFactory.DataDB.VendorDbTypes;
            var Columns = new DVColumnCollection();
            int index = 0;
            DVBaseColumn col = new DVNumericColumn { Data = index, Name = "id", sTitle = "id", Type = EbDbTypes.Decimal, bVisible = false, sWidth = "100px", ClassName = "tdheight" };
            Columns.Add(col);
            foreach (TableColumnMeta column in listNamesAndTypes)
            {
                if (column.Name != "eb_del" && column.Name != "eb_ver_id" && !(column.Name.Contains("_ebbkup")) && column.Name != "eb_push_id" && column.Name != "eb_src_id" && column.Name != "eb_lock" && column.Name != "eb_signin_log_id" && !(column.Control is EbFileUploader))
                {
                    DVBaseColumn _col = null;
                    ControlClass _control = null;
                    bool _autoresolve = false;
                    Align _align = Align.Auto;
                    int charlength = 0;
                    index++;
                    EbDbTypes _RenderType = column.Type.EbDbType;

                    if (column.Control is EbPowerSelect)
                    {
                        _control = new ControlClass
                        {
                            DataSourceId = (column.Control as EbPowerSelect).DataSourceId,
                            ValueMember = (column.Control as EbPowerSelect).ValueMember
                        };
                        if ((column.Control as EbPowerSelect).RenderAsSimpleSelect)
                        {
                            _control.DisplayMember.Add((column.Control as EbPowerSelect).DisplayMember);
                        }
                        else
                        {
                            _control.DisplayMember = (column.Control as EbPowerSelect).DisplayMembers;
                        }
                        _autoresolve = true;
                        _align = Align.Center;
                        _RenderType = EbDbTypes.String;
                    }
                    else if (column.Control is EbTextBox)
                    {
                        if ((column.Control as EbTextBox).TextMode == TextMode.MultiLine)
                        {
                            charlength = 20;
                        }
                    }
                    else if (column.Name == "eb_void")
                    {
                        column.Type = vDbTypes.String;//T or F
                        _RenderType = EbDbTypes.Boolean;
                    }
                    else if (column.Name == "eb_created_by" || column.Name == "eb_lastmodified_by" || column.Name == "eb_loc_id")
                    {
                        _RenderType = EbDbTypes.String;
                    }
                    if (column.Name == "eb_approval")
                    {
                        _col = new DVApprovalColumn
                        {
                            Data = index,
                            Name = column.Name,
                            sTitle = column.Label,
                            Type = EbDbTypes.String,
                            bVisible = true,
                            sWidth = "100px",
                            ClassName = "tdheight",
                            RenderType = EbDbTypes.String,
                            IsCustomColumn = true,
                            FormRefid = request.WebObj.RefId,
                            FormDataId = new List<DVBaseColumn> { col }
                        };
                    }
                    else if (_RenderType == EbDbTypes.String)
                        _col = new DVStringColumn
                        {
                            Data = index,
                            Name = column.Name,
                            sTitle = column.Label,
                            Type = column.Type.EbDbType,
                            bVisible = true,
                            sWidth = "100px",
                            ClassName = "tdheight",
                            ColumnQueryMapping = _control,
                            AutoResolve = _autoresolve,
                            Align = _align,
                            AllowedCharacterLength = charlength,
                            RenderType = _RenderType
                        };
                    else if (_RenderType == EbDbTypes.Int16 || _RenderType == EbDbTypes.Int32 || _RenderType == EbDbTypes.Int64 || _RenderType == EbDbTypes.Double || _RenderType == EbDbTypes.Decimal || _RenderType == EbDbTypes.VarNumeric)
                        _col = new DVNumericColumn
                        {
                            Data = index,
                            Name = column.Name,
                            sTitle = column.Label,
                            Type = column.Type.EbDbType,
                            bVisible = true,
                            sWidth = "100px",
                            ClassName = "tdheight",
                            ColumnQueryMapping = _control,
                            AutoResolve = _autoresolve,
                            Align = _align,
                            AllowedCharacterLength = charlength,
                            RenderType = _RenderType
                        };
                    else if (_RenderType == EbDbTypes.Boolean || _RenderType == EbDbTypes.BooleanOriginal)
                        _col = new DVBooleanColumn
                        {
                            Data = index,
                            Name = column.Name,
                            sTitle = column.Label,
                            Type = column.Type.EbDbType,
                            bVisible = true,
                            sWidth = "100px",
                            ClassName = "tdheight",
                            Align = _align,
                            AllowedCharacterLength = charlength,
                            RenderType = _RenderType
                        };
                    else if (_RenderType == EbDbTypes.DateTime || _RenderType == EbDbTypes.Date || _RenderType == EbDbTypes.Time)
                        _col = new DVDateTimeColumn
                        {
                            Data = index,
                            Name = column.Name,
                            sTitle = column.Label,
                            Type = column.Type.EbDbType,
                            bVisible = true,
                            sWidth = "100px",
                            ClassName = "tdheight",
                            Align = _align,
                            AllowedCharacterLength = charlength,
                            RenderType = _RenderType
                        };

                    Columns.Add(_col);
                }
            }
            List<DVBaseColumn> _formid = new List<DVBaseColumn>() { col };

            Columns.Add(new DVActionColumn
            {
                Data = (index + 1),//index+1 for serial column in datavis service
                Name = "eb_action",
                sTitle = "Action",
                Type = EbDbTypes.String,
                bVisible = true,
                sWidth = "100px",
                ClassName = "tdheight",
                LinkRefId = request.WebObj.RefId,
                LinkType = LinkTypeEnum.Popout,
                FormMode = WebFormDVModes.View_Mode,
                FormId = _formid,
                Align = Align.Center,
                IsCustomColumn = true
            });
            return Columns;
        }

        private DVColumnCollection UpdateDVColumnCollection(List<TableColumnMeta> listNamesAndTypes, CreateWebFormTableRequest request, EbTableVisualization dv)
        {
            IVendorDbTypes vDbTypes = this.EbConnectionFactory.DataDB.VendorDbTypes;
            var Columns = new DVColumnCollection();
            int index = 0;
            foreach (TableColumnMeta column in listNamesAndTypes)
            {
                DVBaseColumn _col = dv.Columns.Find(x => x.Name == column.Name);
                if (_col == null && column.Name != "eb_del" && column.Name != "eb_ver_id" && !(column.Name.Contains("_ebbkup")) && column.Name != "eb_push_id" && column.Name != "eb_src_id" && column.Name != "eb_lock" && column.Name != "eb_signin_log_id" && !(column.Control is EbFileUploader))
                {
                    index++;
                    ControlClass _control = null;
                    bool _autoresolve = false;
                    Align _align = Align.Auto;
                    int charlength = 0;
                    EbDbTypes _RenderType = column.Type.EbDbType;
                    if (column.Control is EbPowerSelect)
                    {
                        _control = new ControlClass
                        {
                            DataSourceId = (column.Control as EbPowerSelect).DataSourceId,
                            ValueMember = (column.Control as EbPowerSelect).ValueMember
                        };
                        if ((column.Control as EbPowerSelect).RenderAsSimpleSelect)
                        {
                            _control.DisplayMember.Add((column.Control as EbPowerSelect).DisplayMember);
                        }
                        else
                        {
                            _control.DisplayMember = (column.Control as EbPowerSelect).DisplayMembers;
                        }
                        _autoresolve = true;
                        _align = Align.Center;
                        _RenderType = EbDbTypes.String;
                    }
                    else if (column.Control is EbTextBox)
                    {
                        if ((column.Control as EbTextBox).TextMode == TextMode.MultiLine)
                        {
                            charlength = 20;
                        }
                    }
                    if (column.Name == "eb_approval")
                    {
                        _col = new DVApprovalColumn
                        {
                            Data = index,
                            Name = column.Name,
                            sTitle = column.Label,
                            Type = EbDbTypes.String,
                            bVisible = true,
                            sWidth = "100px",
                            ClassName = "tdheight",
                            RenderType = EbDbTypes.String,
                            IsCustomColumn = true,
                            FormRefid = request.WebObj.RefId,
                            FormDataId = new List<DVBaseColumn> { dv.Columns.Get("id") }
                        };
                    }
                    else if (_RenderType == EbDbTypes.String)
                        _col = new DVStringColumn
                        {
                            Data = index,
                            Name = column.Name,
                            sTitle = column.Label,
                            Type = column.Type.EbDbType,
                            bVisible = true,
                            sWidth = "100px",
                            ClassName = "tdheight",
                            ColumnQueryMapping = _control,
                            AutoResolve = _autoresolve,
                            Align = _align,
                            AllowedCharacterLength = charlength,
                            RenderType = _RenderType
                        };
                    else if (_RenderType == EbDbTypes.Int16 || _RenderType == EbDbTypes.Int32 || _RenderType == EbDbTypes.Int64 || _RenderType == EbDbTypes.Double || _RenderType == EbDbTypes.Decimal || _RenderType == EbDbTypes.VarNumeric)
                        _col = new DVNumericColumn
                        {
                            Data = index,
                            Name = column.Name,
                            sTitle = column.Label,
                            Type = column.Type.EbDbType,
                            bVisible = true,
                            sWidth = "100px",
                            ClassName = "tdheight",
                            ColumnQueryMapping = _control,
                            AutoResolve = _autoresolve,
                            Align = _align,
                            AllowedCharacterLength = charlength,
                            RenderType = _RenderType
                        };
                    else if (_RenderType == EbDbTypes.Boolean || _RenderType == EbDbTypes.BooleanOriginal)
                        _col = new DVBooleanColumn
                        {
                            Data = index,
                            Name = column.Name,
                            sTitle = column.Label,
                            Type = column.Type.EbDbType,
                            bVisible = true,
                            sWidth = "100px",
                            ClassName = "tdheight",
                            Align = _align,
                            AllowedCharacterLength = charlength,
                            RenderType = _RenderType
                        };
                    else if (_RenderType == EbDbTypes.DateTime || _RenderType == EbDbTypes.Date || _RenderType == EbDbTypes.Time)
                        _col = new DVDateTimeColumn
                        {
                            Data = index,
                            Name = column.Name,
                            sTitle = column.Label,
                            Type = column.Type.EbDbType,
                            bVisible = true,
                            sWidth = "100px",
                            ClassName = "tdheight",
                            Align = _align,
                            AllowedCharacterLength = charlength,
                            RenderType = _RenderType
                        };

                    Columns.Add(_col);
                }
                else
                {
                    if (_col != null)
                    {
                        if (column.Name == "eb_void")
                        {
                            column.Type = vDbTypes.String;//T or F
                            _col.RenderType = EbDbTypes.Boolean;
                        }
                        else if (column.Name == "eb_created_by" || column.Name == "eb_lastmodified_by" || column.Name == "eb_loc_id")
                        {
                            _col.RenderType = EbDbTypes.String;
                        }
                        else
                            _col.RenderType = column.Type.EbDbType;

                        if (column.Control is EbPowerSelect)
                        {
                            var _control = new ControlClass
                            {
                                DataSourceId = (column.Control as EbPowerSelect).DataSourceId,
                                ValueMember = (column.Control as EbPowerSelect).ValueMember
                            };
                            if ((column.Control as EbPowerSelect).RenderAsSimpleSelect)
                            {
                                _control.DisplayMember.Add((column.Control as EbPowerSelect).DisplayMember);
                            }
                            else
                            {
                                _control.DisplayMember = (column.Control as EbPowerSelect).DisplayMembers;
                            }
                            _col.ColumnQueryMapping = _control;
                            _col.AutoResolve = true;
                            _col.Align = Align.Center;
                            _col.RenderType = EbDbTypes.String;
                        }
                        else if (column.Control is EbTextBox)
                        {
                            if ((column.Control as EbTextBox).TextMode == TextMode.MultiLine)
                            {
                                _col.AllowedCharacterLength = 20;
                            }
                        }
                        _col.Data = ++index;
                        Columns.Add(_col);
                    }
                }

            }

            Columns.Add(dv.Columns.Get("id"));
            DVBaseColumn Col = dv.Columns.Get("eb_action");
            DVBaseColumn actcol = null;
            if (Col == null || Col is DVStringColumn)
            {
                actcol = new DVActionColumn
                {
                    Name = "eb_action",
                    sTitle = "Action",
                    Type = EbDbTypes.String,
                    bVisible = true,
                    sWidth = "100px",
                    ClassName = "tdheight",
                    LinkRefId = request.WebObj.RefId,
                    LinkType = LinkTypeEnum.Popout,
                    FormMode = WebFormDVModes.View_Mode,
                    FormId = new List<DVBaseColumn> { dv.Columns.Get("id") },
                    Align = Align.Center,
                    IsCustomColumn = true
                };
            }
            else
                actcol = Col;
            actcol.Data = ++index;
            Columns.Add(actcol);
            return Columns;
        }

        private void UpdateOrderByObject(ref EbTableVisualization dv)
        {
            List<DVBaseColumn> _orderby = new List<DVBaseColumn>(dv.OrderBy);
            string[] _array = dv.Columns.Select(x => x.Name).ToArray();
            int index = -1;
            foreach (DVBaseColumn col in _orderby)
            {
                index++;
                if (!_array.Contains(col.Name))
                    dv.OrderBy.RemoveAll(x => x.Name == col.Name);
                else
                    dv.OrderBy[index].Data = dv.Columns.FindAll(x => x.Name == col.Name)[0].Data;
            }
        }

        private void UpdateRowGroupObject(ref EbTableVisualization dv)
        {
            List<RowGroupParent> _rowgroupColl = new List<RowGroupParent>(dv.RowGroupCollection);
            string[] _array = dv.Columns.Select(x => x.Name).ToArray();
            int index = -1;
            foreach (RowGroupParent _rowgroup in _rowgroupColl)
            {
                index++; int j = -1;
                foreach (DVBaseColumn col in _rowgroup.RowGrouping)
                {
                    j++;
                    if (!_array.Contains(col.Name))
                        dv.RowGroupCollection[index].RowGrouping.RemoveAll(x => x.Name == col.Name);
                    else
                        dv.RowGroupCollection[index].RowGrouping[j].Data = dv.Columns.FindAll(x => x.Name == col.Name)[0].Data;
                }
                j = -1;
                foreach (DVBaseColumn col in _rowgroup.OrderBy)
                {
                    j++;
                    if (!_array.Contains(col.Name))
                        dv.RowGroupCollection[index].OrderBy.RemoveAll(x => x.Name == col.Name);
                    else
                        dv.RowGroupCollection[index].OrderBy[j].Data = dv.Columns.FindAll(x => x.Name == col.Name)[0].Data;
                }

            }
        }

        private void UpdateInfowindowObject(ref EbTableVisualization dv)
        {
            List<DVBaseColumn> _ColColl = dv.Columns.FindAll(x => x.InfoWindow.Count > 0);
            string[] _array = dv.Columns.Select(x => x.Name).ToArray();
            foreach (DVBaseColumn col in _ColColl)
            {
                int idx = dv.Columns.FindIndex(x => x.Name == col.Name);
                int index = -1;
                foreach (DVBaseColumn _col in col.InfoWindow)
                {
                    index++;
                    if (!_array.Contains(_col.Name))
                    {
                        dv.Columns[idx].InfoWindow.RemoveAll(x => x.Name == _col.Name);
                    }
                    else
                    {
                        dv.Columns[idx].InfoWindow[index].Data = dv.Columns.FindAll(x => x.Name == _col.Name)[0].Data;
                    }
                }
            }
        }

        //================================== GET RECORD FOR RENDERING ================================================

        public GetRowDataResponse Any(GetRowDataRequest request)
        {
            GetRowDataResponse _dataset = new GetRowDataResponse();
            try
            {
                Console.WriteLine("Requesting for WebFormData( Refid : " + request.RefId + ", Rowid : " + request.RowId + " ).................");
                EbWebForm form = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);
                form.TableRowId = request.RowId;
                if (form.TableRowId > 0)
                    form.RefreshFormData(EbConnectionFactory.DataDB, this);
                else
                {
                    if (form.UserObj.LocationIds.Contains(-1) || form.UserObj.LocationIds.Contains(request.CurrentLoc))
                    {
                        if (form.SolutionObj.Locations.ContainsKey(request.CurrentLoc))
                            form.UserObj.Preference.DefaultLocation = request.CurrentLoc;
                    }
                    form.GetEmptyModel();
                }
                if (form.SolutionObj.SolutionSettings != null && form.SolutionObj.SolutionSettings.SignupFormRefid != string.Empty && form.SolutionObj.SolutionSettings.SignupFormRefid == form.RefId)
                {
                }
                else if (form.SolutionObj.SolutionSettings != null && form.SolutionObj.SolutionSettings.UserTypeForms != null && form.SolutionObj.SolutionSettings.UserTypeForms.Any(x => x.RefId == form.RefId))
                {
                }
                //bot c
                //else if (!(form.HasPermission(OperationConstants.VIEW, request.CurrentLoc) || form.HasPermission(OperationConstants.NEW, request.CurrentLoc) || form.HasPermission(OperationConstants.EDIT, request.CurrentLoc)))
                //{
                //    throw new FormException("Error in loading data. Access Denied.", (int)HttpStatusCodes.UNAUTHORIZED, "Access Denied for rowid " + form.TableRowId + " , current location " + form.LocationId, string.Empty);
                //}
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper()
                {
                    FormData = form.FormData,
                    Status = (int)HttpStatusCodes.OK,
                    Message = "Success"
                });
                Console.WriteLine("Returning from GetRowData Service : Success");
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in GetRowData Service \nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace : " + ex.StackTrace);
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper() { Message = ex.Message, Status = ex.ExceptionCode, MessageInt = ex.MessageInternal, StackTraceInt = ex.StackTraceInternal });
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in GetRowData Service \nMessage : " + ex.Message + "\nStackTrace : " + ex.StackTrace);
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper() { Message = "Something went wrong", Status = (int)HttpStatusCodes.INTERNAL_SERVER_ERROR, MessageInt = ex.Message, StackTraceInt = ex.StackTrace });
            }
            return _dataset;
        }

        public GetPrefillDataResponse Any(GetPrefillDataRequest request)
        {
            Console.WriteLine("Start GetPrefillData");
            GetPrefillDataResponse _dataset = new GetPrefillDataResponse();
            try
            {
                EbWebForm form = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);
                form.TableRowId = 0;
                form.RefreshFormData(EbConnectionFactory.DataDB, this, request.Params);
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper { FormData = form.FormData, Status = (int)HttpStatusCodes.OK, Message = "Success" });
                Console.WriteLine("End GetPrefillData : Success");
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in GetPrefillData Service \nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace : " + ex.StackTrace);
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper { Message = ex.Message, Status = ex.ExceptionCode, MessageInt = ex.MessageInternal, StackTraceInt = ex.StackTraceInternal });
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception in GetPrefillData Service \nMessage : " + e.Message + "\nStackTrace : " + e.StackTrace);
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper { Message = "Something went wrong.", Status = (int)HttpStatusCodes.INTERNAL_SERVER_ERROR, MessageInt = e.Message, StackTraceInt = e.StackTrace });
            }
            return _dataset;
        }

        public GetExportFormDataResponse Any(GetExportFormDataRequest request)
        {
            Console.WriteLine("Start GetExportFormData");
            GetExportFormDataResponse _dataset = new GetExportFormDataResponse();
            try
            {
                EbWebForm sourceForm = this.GetWebFormObject(request.SourceRefId, request.UserAuthId, request.SolnId);
                sourceForm.TableRowId = request.SourceRowId;

                EbWebForm destForm = this.GetWebFormObject(request.DestRefId, null, null);
                destForm.UserObj = sourceForm.UserObj;
                destForm.SolutionObj = sourceForm.SolutionObj;
                if (request.SourceRowId > 0)
                    sourceForm.GetImportData(EbConnectionFactory.DataDB, this, destForm);
                else
                    destForm.GetEmptyModel();
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper { FormData = destForm.FormData, Status = (int)HttpStatusCodes.OK, Message = "Success" });
                Console.WriteLine("End GetExportFormData : Success");
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in GetExportFormData Service \nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace : " + ex.StackTrace);
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper { Message = ex.Message, Status = ex.ExceptionCode, MessageInt = ex.MessageInternal, StackTraceInt = ex.StackTraceInternal });
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception in GetExportFormData Service \nMessage : " + e.Message + "\nStackTrace : " + e.StackTrace);
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper { Message = "Something went wrong.", Status = (int)HttpStatusCodes.INTERNAL_SERVER_ERROR, MessageInt = e.Message, StackTraceInt = e.StackTrace });
            }
            return _dataset;
        }

        public GetFormData4MobileResponse Any(GetFormData4MobileRequest request)
        {
            Console.WriteLine("Start GetFormData4Mobile");
            GetFormData4MobileResponse resp = null;
            try
            {
                EbWebForm form = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);
                form.TableRowId = request.DataId;
                List<Param> data = null;
                if (form.TableRowId > 0)
                    data = form.GetFormData4Mobile(EbConnectionFactory.DataDB, this);
                resp = new GetFormData4MobileResponse() { Params = data, Status = (int)HttpStatusCodes.OK, Message = "Success" };
                Console.WriteLine("End GetFormData4Mobile : Success");
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in GetFormData4Mobile Service \nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace : " + ex.StackTrace);
                resp = new GetFormData4MobileResponse() { Status = (int)HttpStatusCodes.INTERNAL_SERVER_ERROR, Message = $"{ex.Message} {ex.MessageInternal}" };
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception in GetFormData4Mobile Service \nMessage : " + e.Message + "\nStackTrace : " + e.StackTrace);
                resp = new GetFormData4MobileResponse() { Status = (int)HttpStatusCodes.INTERNAL_SERVER_ERROR, Message = $"{e.Message} {e.StackTrace}" };
            }
            return resp;
        }

        public GetImportDataResponse Any(GetImportDataRequest request)
        {
            WebformDataWrapper data;
            try
            {
                Console.WriteLine("Start ImportFormData");
                EbWebForm form = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);
                form.ImportData(EbConnectionFactory.DataDB, this, request.Params, request.Trigger, request.RowId);
                data = new WebformDataWrapper { FormData = form.FormData, Status = (int)HttpStatusCodes.OK, Message = "Success" };
                Console.WriteLine("End ImportFormData : Success");
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in GetImportDataRequest Service \nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace : " + ex.StackTrace);
                data = new WebformDataWrapper { Status = ex.ExceptionCode, Message = ex.Message, MessageInt = ex.MessageInternal, StackTraceInt = ex.StackTraceInternal };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in GetImportDataRequest Service \nMessage : " + ex.Message + "\nStackTrace" + ex.StackTrace);
                data = new WebformDataWrapper { Status = (int)HttpStatusCodes.INTERNAL_SERVER_ERROR, Message = "Exception in GetImportDataRequest", MessageInt = ex.Message, StackTraceInt = ex.StackTrace };
            }
            return new GetImportDataResponse() { FormDataWrap = JsonConvert.SerializeObject(data) };
        }

        public GetDynamicGridDataResponse Any(GetDynamicGridDataRequest request)
        {
            WebformDataWrapper data;
            try
            {
                Console.WriteLine("Start GetDynamicGridData");
                EbWebForm form = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);
                form.TableRowId = request.RowId;
                WebformData wfd = form.GetDynamicGridData(EbConnectionFactory.DataDB, this, request.SourceId, request.Target);
                data = new WebformDataWrapper { FormData = wfd, Status = (int)HttpStatusCodes.OK, Message = "Success" };
                Console.WriteLine("End GetDynamicGridData : Success");
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in GetDynamicGridDataRequest Service \nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace : " + ex.StackTrace);
                data = new WebformDataWrapper { Status = ex.ExceptionCode, Message = ex.Message, MessageInt = ex.MessageInternal, StackTraceInt = ex.StackTraceInternal };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in GetDynamicGridDataRequest Service \nMessage : " + ex.Message + "\n" + ex.StackTrace);
                data = new WebformDataWrapper { Status = (int)HttpStatusCodes.INTERNAL_SERVER_ERROR, Message = "Exception in GetDynamicGridDataRequest", MessageInt = ex.Message, StackTraceInt = ex.StackTrace };
            }
            return new GetDynamicGridDataResponse() { FormDataWrap = JsonConvert.SerializeObject(data) };
        }

        public ExecuteSqlValueExprResponse Any(ExecuteSqlValueExprRequest request)
        {
            Console.WriteLine("Start ExecuteSqlValueExpr");
            EbWebForm form = this.GetWebFormObject(request.RefId, null, null);
            string val = form.ExecuteSqlValueExpression(EbConnectionFactory.DataDB, this, request.Params, request.Trigger);
            Console.WriteLine("End ExecuteSqlValueExpr");
            return new ExecuteSqlValueExprResponse() { Data = val };
        }

        public GetDataPusherJsonResponse Any(GetDataPusherJsonRequest request)
        {
            Console.WriteLine("Start GetDataPusherJson");
            EbWebForm form = this.GetWebFormObject(request.RefId, null, null);
            string val = form.GetDataPusherJson();
            Console.WriteLine("End GetDataPusherJson");
            return new GetDataPusherJsonResponse() { Json = val };
        }


        private EbWebForm GetWebFormObject(string RefId, string UserAuthId, string SolnId)
        {
            EbWebForm _form = this.Redis.Get<EbWebForm>(RefId);
            if (_form == null)
            {
                var myService = base.ResolveService<EbObjectService>();
                EbObjectParticularVersionResponse formObj = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = RefId });
                _form = EbSerializers.Json_Deserialize(formObj.Data[0].Json);
                this.Redis.Set<EbWebForm>(RefId, _form);
            }
            _form.RefId = RefId;
            if (UserAuthId != null)
                _form.UserObj = this.Redis.Get<User>(UserAuthId);
            if (SolnId != null)
                _form.SolutionObj = this.GetSolutionObject(SolnId);
            _form.AfterRedisGet(this);
            return _form;
        }

        public DoUniqueCheckResponse Any(DoUniqueCheckRequest Req)
        {
            string query = string.Format("SELECT id FROM {0} WHERE {1} = :value;", Req.TableName, Req.Field);
            EbControl obj = Activator.CreateInstance(typeof(ExpressBase.Objects.Margin).Assembly.GetType("ExpressBase.Objects." + Req.TypeS, true), true) as EbControl;
            DbParameter[] param = {
                this.EbConnectionFactory.DataDB.GetNewParameter("value",obj.EbDbType, Req.Value)
            };
            EbDataTable datatbl = this.EbConnectionFactory.DataDB.DoQuery(query, param);
            return new DoUniqueCheckResponse { NoRowsWithSameValue = datatbl.Rows.Count };
        }

        public GetDictionaryValueResponse Any(GetDictionaryValueRequest request)
        {
            Dictionary<string, string> Dict = new Dictionary<string, string>();
            string qry = @"SELECT k.key, v.value 
                            FROM 
	                            eb_keys k, eb_languages l, eb_keyvalue v
                            WHERE
	                            k.id = v.key_id AND
	                            l.id = v.lang_id AND
	                            k.key IN ({0})
	                            AND l.language LIKE '%({1})';";

            string temp = string.Empty;
            foreach (string t in request.Keys)
            {
                temp += "'" + t + "',";
            }
            qry = string.Format(qry, temp.Substring(0, temp.Length - 1), request.Locale);
            EbDataTable datatbl = this.EbConnectionFactory.DataDB.DoQuery(qry, new DbParameter[] { });

            foreach (EbDataRow dr in datatbl.Rows)
            {
                Dict.Add(dr["key"].ToString(), dr["value"].ToString());
            }

            return new GetDictionaryValueResponse { Dict = Dict };
        }

        //======================================= INSERT OR UPDATE OR DELETE RECORD =============================================

        public InsertDataFromWebformResponse Any(InsertDataFromWebformRequest request)
        {
            try
            {
                DateTime startdt = DateTime.Now;
                Console.WriteLine("Insert/Update WebFormData : start - " + startdt);
                EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);
                FormObj.TableRowId = request.RowId;
                FormObj.FormData = request.FormData;
                FormObj.LocationId = request.CurrentLoc;

                //string Operation = OperationConstants.NEW;
                //if (request.RowId > 0)
                //    Operation = OperationConstants.EDIT;
                //if (!FormObj.HasPermission(Operation, request.CurrentLoc))////bot c
                //    return new InsertDataFromWebformResponse { Status = (int)HttpStatusCodes.FORBIDDEN, Message = "Access denied to save this data entry!", MessageInt = "Access denied" };

                Console.WriteLine("Insert/Update WebFormData : MergeFormData start - " + DateTime.Now);
                FormObj.MergeFormData();
                Console.WriteLine("Insert/Update WebFormData : Save start - " + DateTime.Now);
                string r = FormObj.Save(EbConnectionFactory.DataDB, this);
                Console.WriteLine("Insert/Update WebFormData : AfterExecutionIfUserCreated start - " + DateTime.Now);
                FormObj.AfterExecutionIfUserCreated(this, this.EbConnectionFactory.EmailConnection, MessageProducer3);
                Console.WriteLine("Insert/Update WebFormData end : Execution Time = " + (DateTime.Now - startdt).TotalMilliseconds);
                return new InsertDataFromWebformResponse()
                {
                    Message = "Success",
                    RowId = FormObj.TableRowId,
                    FormData = JsonConvert.SerializeObject(FormObj.FormData),
                    RowAffected = 1,
                    AffectedEntries = r,
                    Status = (int)HttpStatusCodes.OK,
                };
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in Insert/Update WebFormData\nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace" + ex.StackTrace);
                return new InsertDataFromWebformResponse()
                {
                    Message = ex.Message,
                    Status = ex.ExceptionCode,
                    MessageInt = ex.MessageInternal,
                    StackTraceInt = ex.StackTraceInternal
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in Insert/Update WebFormData\nMessage : " + ex.Message + "\nStackTrace : " + ex.StackTrace);
                return new InsertDataFromWebformResponse()
                {
                    Message = "Something went wrong",
                    Status = (int)HttpStatusCodes.INTERNAL_SERVER_ERROR,
                    MessageInt = ex.Message,
                    StackTraceInt = ex.StackTrace
                };
            }
        }

        public DeleteDataFromWebformResponse Any(DeleteDataFromWebformRequest request)
        {
            EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, null);
            foreach (int _rowId in request.RowId)
            {
                FormObj.TableRowId = _rowId;
                FormObj.Delete(EbConnectionFactory.DataDB);
            }
            return new DeleteDataFromWebformResponse
            {
                RowAffected = request.RowId.Count()
            };
        }

        public CancelDataFromWebformResponse Any(CancelDataFromWebformRequest request)
        {
            EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, null);
            FormObj.TableRowId = request.RowId;
            return new CancelDataFromWebformResponse
            {
                RowAffected = FormObj.Cancel(EbConnectionFactory.DataDB)
            };
        }

        public InsertOrUpdateFormDataResp Any(InsertOrUpdateFormDataRqst request)
        {
            try
            {
                Console.WriteLine("InsertOrUpdateFormDataRqst Service start");
                EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);
                FormObj.TableRowId = request.RecordId;
                FormObj.LocationId = request.LocId;
                Console.WriteLine("InsertOrUpdateFormDataRqst PrepareWebFormData start : " + DateTime.Now);
                FormObj.PrepareWebFormData(this.EbConnectionFactory.DataDB, this, request.PushJson, request.FormGlobals);
                Console.WriteLine("InsertOrUpdateFormDataRqst Save start : " + DateTime.Now);
                string r = FormObj.Save(this.EbConnectionFactory.DataDB, this, request.TransactionConnection);
                Console.WriteLine("InsertOrUpdateFormDataRqst returning");
                return new InsertOrUpdateFormDataResp() { Status = (int)HttpStatusCodes.OK, Message = "success", RecordId = FormObj.TableRowId };
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in InsertOrUpdateFormDataRqst\nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace : " + ex.StackTrace);
                return new InsertOrUpdateFormDataResp() { Status = ex.ExceptionCode, Message = ex.Message };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in InsertOrUpdateFormDataRqst\nMessage" + ex.Message + "\nStackTrace" + ex.StackTrace);
                return new InsertOrUpdateFormDataResp() { Status = (int)HttpStatusCodes.INTERNAL_SERVER_ERROR, Message = ex.Message };
            }
        }

        public InsertBatchDataResponse Any(InsertBatchDataRequest request)
        {
            try
            {
                Console.WriteLine("InsertBatchDataRequest Service start");
                EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);
                FormObj.LocationId = request.LocId;
                List<int> Ids = FormObj.ProcessBatchRequest(request.Data, this.EbConnectionFactory.DataDB, this, request.TransactionConnection);
                Console.WriteLine("InsertBatchDataRequest returning");
                return new InsertBatchDataResponse() { Status = (int)HttpStatusCodes.OK, Message = "success", RecordIds = Ids };
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in InsertOrUpdateFormDataRqst\nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace : " + ex.StackTrace);
                return new InsertBatchDataResponse() { Status = ex.ExceptionCode, Message = ex.Message };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in InsertOrUpdateFormDataRqst\nMessage" + ex.Message + "\nStackTrace" + ex.StackTrace);
                return new InsertBatchDataResponse() { Status = (int)HttpStatusCodes.INTERNAL_SERVER_ERROR, Message = ex.Message };
            }
        }




        //================================= FORMULA AND VALIDATION =================================================

        public WebformData CalcFormula(WebformData _formData, EbWebForm _formObj)
        {
            Dictionary<int, EbControlWrapper> ctrls = new Dictionary<int, EbControlWrapper>();
            BeforeSaveHelper.GetControlsAsDict(_formObj, "FORM", ctrls);
            List<int> ExeOrder = GetExecutionOrder(ctrls);

            for (int i = 0; i < ExeOrder.Count; i++)
            {
                EbControlWrapper cw = ctrls[ExeOrder[i]];
                Script valscript = CSharpScript.Create<dynamic>(
                    cw.Control.ValueExpr.Code,
                    ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic", "System", "System.Collections.Generic",
                    "System.Diagnostics", "System.Linq"),
                    globalsType: typeof(FormGlobals)
                );
                valscript.Compile();

                FormAsGlobal g = GlobalsGenerator.GetFormAsGlobal(_formObj, _formData);
                FormGlobals globals = new FormGlobals() { form = g };
                var result = (valscript.RunAsync(globals)).Result.ReturnValue;

                _formData.MultipleTables[cw.TableName][0].Columns.Add(new SingleColumn
                {
                    Name = cw.Control.Name,
                    Type = (int)cw.Control.EbDbType,
                    Value = result
                });
            }
            return _formData;
        }

        private List<int> GetExecutionOrder(Dictionary<int, EbControlWrapper> ctrls)
        {
            List<int> CalcFlds = new List<int>() { 1, 2, 4 };//
            List<int> ExeOrd = new List<int>();
            List<KeyValuePair<int, int>> dpndcy = new List<KeyValuePair<int, int>>();
            for (int i = 0; i < CalcFlds.Count; i++)
            {
                if (ctrls[CalcFlds[i]].Control.ValueExpr.Code.Contains("FORM"))//testing purpose
                {
                    for (int j = 0; j < CalcFlds.Count; j++)
                    {
                        if (i != j)
                        {
                            if (ctrls[CalcFlds[i]].Control.ValueExpr.Code.Contains(ctrls[CalcFlds[i]].Path))
                                dpndcy.Add(new KeyValuePair<int, int>(i, j));
                        }
                    }
                }
            }
            int count = 0;
            while (dpndcy.Count > 0 && count < CalcFlds.Count)
            {
                for (int i = 0; i < CalcFlds.Count; i++)
                {
                    var t = dpndcy.FindIndex(x => x.Key == CalcFlds[i]);
                    if (t == -1)
                    {
                        ExeOrd.Add(CalcFlds[i]);
                        dpndcy.RemoveAll(x => x.Value == CalcFlds[i]);
                    }
                }
                count++;
            }

            return ExeOrd;
        }

        //incomplete
        public bool ValidateFormData(WebformData _formData, EbWebForm _formObj)
        {
            var engine = new ScriptEngine();
            ObjectInstance globals = GetJsFormGlobal(_formData, _formObj, engine);
            engine.SetGlobalValue("FORM", globals);
            List<EbValidator> warnings = new List<EbValidator>();
            List<EbValidator> errors = new List<EbValidator>();
            Dictionary<int, EbControlWrapper> ctrls = new Dictionary<int, EbControlWrapper>();
            BeforeSaveHelper.GetControlsAsDict(_formObj, "FORM", ctrls);
            foreach (KeyValuePair<int, EbControlWrapper> ctrl in ctrls)
            {
                for (int i = 0; i < ctrl.Value.Control.Validators.Count; i++)
                {
                    EbValidator v = ctrl.Value.Control.Validators[i];
                    if (!v.IsDisabled)
                    {
                        string fn = v.Name + ctrl.Key;
                        engine.Evaluate("function " + fn + "() { " + v.Script.Code + " }");
                        if (!engine.CallGlobalFunction<bool>(fn))
                        {
                            if (v.IsWarningOnly)
                                warnings.Add(v);
                            else
                                errors.Add(v);
                        }
                    }
                }
            }
            return (errors.Count > 0);
        }


        //get formdata as globals for Jurassic script engine
        private ObjectInstance GetJsFormGlobal(WebformData _formData, EbControlContainer _container, ScriptEngine _engine, ObjectInstance _globals = null)
        {
            if (_globals == null)
            {
                _globals = _engine.Object.Construct();
            }
            if (_formData.MultipleTables.ContainsKey(_container.TableName))
            {
                if (_formData.MultipleTables[_container.TableName].Count > 0)
                {
                    foreach (EbControl control in _container.Controls)
                    {
                        if (_container is EbDataGrid)
                        {
                            EbDataGrid dg = _container as EbDataGrid;
                            ArrayInstance a = _engine.Array.Construct(_formData.MultipleTables[_container.TableName].Count);
                            _globals[control.Name] = a;
                            for (int i = 0; i < _formData.MultipleTables[_container.TableName].Count; i++)
                            {
                                ObjectInstance g = _engine.Object.Construct();
                                a[i] = g;
                                foreach (EbControl c in dg.Controls)
                                {
                                    g[c.Name] = GetDataByControlName(_formData, dg.TableName, c.Name, i);
                                }
                            }
                        }
                        else if (control is EbControlContainer)
                        {
                            ObjectInstance g = _engine.Object.Construct();
                            _globals[control.Name] = g;
                            g = GetJsFormGlobal(_formData, control as EbControlContainer, _engine, g);
                        }
                        else
                        {
                            _globals[control.Name] = GetDataByControlName(_formData, _container.TableName, control.Name, 0);
                        }
                    }
                }
            }
            return _globals;
        }

        private dynamic GetDataByControlName(WebformData _formData, string _table, string _column, int _row = 0)
        {
            try
            {
                return _formData.MultipleTables[_table][_row][_column];
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception!!! : " + e.Message);
                return null;
            }
        }


        private void JurassicTest()
        {
            //var engine = new ScriptEngine();
            ////engine.Execute("console.log('testing')");
            ////engine.SetGlobalValue("interop", 15);

            //var segment = engine.Object.Construct();
            //segment["type"] = "Feature";
            //segment["properties"] = engine.Object.Construct();
            //var geometry = engine.Object.Construct();
            //geometry["type"] = "LineString";
            //geometry["coordinates"] = engine.Array.Construct(
            //  engine.Array.Construct(-37.3, 121.5),
            //  engine.Array.Construct(-38.1, 122.6)
            //);
            //segment["geometry"] = geometry;

            //engine.SetGlobalValue("form", segment);
            //engine.SetGlobalValue("console", new Jurassic.Library.FirebugConsole(engine));

            //engine.Execute("console.log(form.properties.type)");

            var engine2 = new ScriptEngine();
            engine2.Evaluate("function test(a, b) { if(a%2 === 0) return true;else return false; }");
            Console.WriteLine(engine2.CallGlobalFunction<bool>("test", 5, 6));
        }

        private void CSTest()
        {
            //FormGlobals temp = new FormGlobals();

            //ListNTV t1 = new ListNTV() {
            //    Columns = new List<NTV>() {
            //    new NTV { Name = "demo11", Type = EbDbTypes.String, Value = "febin11" }
            //} };
            //ListNTV t2 = new ListNTV() {
            //    Columns = new List<NTV>() {
            //    new NTV { Name = "demo22", Type = EbDbTypes.String, Value = "febin22" },
            //    new NTV { Name = "demo33", Type = EbDbTypes.String, Value = "febin33" }
            //} };
            //ListNTV t3 = new ListNTV()
            //{
            //    Columns = new List<NTV>() {
            //    new NTV { Name = "demo44", Type = EbDbTypes.String, Value = "febin44" },
            //    new NTV { Name = "demo55", Type = EbDbTypes.String, Value = "febin55" }
            //}
            //};

            //temp.FORM.Rows.Add(t1);
            //var xxx = new FormAsGlobal
            //{
            //    Name = "demo66",
            //    Rows = new List<ListNTV> { t3 },
            //    Containers = new List<FormAsGlobal>()
            //};
            //temp.FORM.Containers.Add(new FormAsGlobal
            //{
            //    Name = "demo33",
            //    Rows = new List<ListNTV> { t2},
            //    Containers = new List<FormAsGlobal>() { xxx}
            //});

            //var xxxx = temp.FORM.demo33.demo22;

            string CsCode = "return FORM.demo33.demo22;";
            Script valscript = CSharpScript.Create<dynamic>(
                CsCode,
                ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic", "System", "System.Collections.Generic",
                "System.Diagnostics", "System.Linq"),
                globalsType: typeof(FormGlobals));

            valscript.Compile();
            FormGlobals globals = new FormGlobals();
            var result = (valscript.RunAsync(globals)).Result.ReturnValue;

        }




        //===================================== AUDIT TRAIL ========================================================

        public GetAuditTrailResponse Any(GetAuditTrailRequest request)
        {
            try
            {
                Console.WriteLine("GetAuditTrail Service start. RefId : " + request.FormId + "\nDataId : " + request.RowId);
                EbWebForm FormObj = this.GetWebFormObject(request.FormId, request.UserAuthId, request.SolnId);
                FormObj.TableRowId = request.RowId;
                string temp = FormObj.GetAuditTrail(EbConnectionFactory.DataDB, this);
                Console.WriteLine("GetAuditTrail Service end");
                return new GetAuditTrailResponse() { Json = temp };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in GetAuditTrail Service\nMessage : " + ex.Message + "\nStackTrace : " + ex.StackTrace);
                throw new FormException("Terminated GetAuditTrail. Check servicestack log for stack trace.");
            }
        }

        //=============================================== MISCELLANEOUS ====================================================

        public GetDesignHtmlResponse Post(GetDesignHtmlRequest request)
        {
            var myService = base.ResolveService<EbObjectService>();
            EbObjectParticularVersionResponse formObj = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });

            EbUserControl _uc = EbSerializers.Json_Deserialize(formObj.Data[0].Json);
            _uc.AfterRedisGet(this);
            _uc.VersionNumber = formObj.Data[0].VersionNumber;//Version number(w) in EbObject is not updated when it is commited
            string _temp = _uc.GetHtml();

            return new GetDesignHtmlResponse { Html = _temp };
        }
        public GetCtrlsFlatResponse Post(GetCtrlsFlatRequest request)
        {
            EbWebForm form = this.GetWebFormObject(request.RefId, null, null);

            IEnumerable<EbControl> ctrls = form.Controls.FlattenEbControls();

            return new GetCtrlsFlatResponse { Controls = ctrls.ToList<EbControl>() };
        }

        public CheckEmailConAvailableResponse Post(CheckEmailConAvailableRequest request)
        {
            bool isAvail = false;
            if (this.EbConnectionFactory.EmailConnection != null)
                isAvail = this.EbConnectionFactory.EmailConnection.Primary != null;
            return new CheckEmailConAvailableResponse { ConnectionAvailable = isAvail };
        }


        public GetDashBoardUserCtrlResponse Post(GetDashBoardUserCtrlRequest Request)
        {
            EbUserControl _ucObj = this.Redis.Get<EbUserControl>(Request.RefId);
            if (_ucObj == null)
            {
                var myService = base.ResolveService<EbObjectService>();
                EbObjectParticularVersionResponse formObj = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = Request.RefId });
                _ucObj = EbSerializers.Json_Deserialize(formObj.Data[0].Json);
                this.Redis.Set<EbUserControl>(Request.RefId, _ucObj);
            }
            //_ucObj.AfterRedisGet(this);
            _ucObj.SetDataObjectControl(this.EbConnectionFactory.DataDB, this, Request.Param);
            _ucObj.IsRenderMode = true;
            return new GetDashBoardUserCtrlResponse()
            {
                UcObjJson = EbSerializers.Json_Serialize(_ucObj),
                UcHtml = _ucObj.GetHtml()
            };
        }

        public GetDistinctValuesResponse Get(GetDistinctValuesRequest request)
        {
            GetDistinctValuesResponse resp = new GetDistinctValuesResponse() { Suggestions = new List<string>() };
            try
            {
                string query = EbConnectionFactory.DataDB.EB_GET_DISTINCT_VALUES
                .Replace("@ColumName", request.ColumnName)
                .Replace("@TableName", request.TableName);
                EbDataTable table = EbConnectionFactory.DataDB.DoQuery(query);

                int capacity = table.Rows.Count;

                for (int i = 0; i < capacity; i++)
                {
                    resp.Suggestions.Add(table.Rows[i][0].ToString());
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("EXCEPTION: Get suggestions for EbTextBox(AutoSuggestion)\nMessage: " + e.Message);
            }
            return resp;

        }

        public UpdateAllFormTablesResponse Post(UpdateAllFormTablesRequest request)
        {
            string msg = $"Start* UpdateAllFormTables {DateTime.Now}\n\n";
            try
            {
                User u = this.Redis.Get<User>(request.UserAuthId);
                if (u.Roles.Contains(SystemRoles.SolutionOwner.ToString()))
                {
                    string Qry = @"SELECT refid,display_name,obj_json FROM (
				                        SELECT 
					                        EO.id, EOV.refid, EO.display_name, EOV.obj_json
				                        FROM
					                        eb_objects EO
				                        LEFT JOIN 
					                        eb_objects_ver EOV ON (EOV.eb_objects_id = EO.id)
				                        LEFT JOIN
					                        eb_objects_status EOS ON (EOS.eb_obj_ver_id = EOV.id)
				                        WHERE
					                        COALESCE(EO.eb_del, 'F') = 'F'
				                        AND EO.obj_type = 0 AND 
					                        EOS.id = ANY( Select MAX(id) from eb_objects_status EOS Where EOS.eb_obj_ver_id = EOV.id)
				                        ) OD 
                                    LEFT JOIN eb_objects2application EO2A ON (EO2A.obj_id = OD.id)
	                                    WHERE COALESCE(EO2A.eb_del, 'F') = 'F' LIMIT 500;";

                    EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(Qry);
                    msg += $"Form Objects Count : {dt.Rows.Count} \n";
                    foreach (EbDataRow dr in dt.Rows)
                    {
                        if (dr.IsDBNull(2))
                            msg += $"\n\nNull Json found, Name : {dr[1].ToString()}";
                        else
                        {
                            EbWebForm F = null;
                            try
                            {
                                F = EbSerializers.Json_Deserialize<EbWebForm>(dr[2].ToString());
                            }
                            catch (Exception ex)
                            {
                                msg += $"\n\nDeserialization Failed, Name : {dr[1].ToString()}, Message : {ex.Message}";
                            }
                            if (F != null)
                            {
                                F.AutoDeployTV = false;
                                try
                                {
                                    this.Any(new CreateWebFormTableRequest { WebObj = F });
                                    msg += $"\n\nSuccess   RefId : {dr[0].ToString()}, Name : {dr[1].ToString()} ";
                                }
                                catch (Exception e)
                                {
                                    msg += $"\n\nWarning   RefId : {dr[0].ToString()}, Name : {dr[1].ToString()}, Message : {e.Message} ";
                                }
                            }
                        }
                    }
                    msg += $"\n\nEnd* UpdateAllFormTables {DateTime.Now}";
                }
            }
            catch (Exception e)
            {
                msg += e.Message;
            }
            Console.WriteLine(msg);
            return new UpdateAllFormTablesResponse() { Message = msg };
        }

        public GetAllRolesResponse Get(GetAllRolesRequest Req)
        {
            string query = "SELECT id, role_name FROM eb_roles WHERE COALESCE(eb_del, 'F') = 'F';";
            EbDataTable datatbl = this.EbConnectionFactory.DataDB.DoQuery(query);
            Dictionary<int, string> t = new Dictionary<int, string>();
            foreach (var dr in datatbl.Rows)
            {
                t.Add(Convert.ToInt32(dr[0]), dr[1].ToString());
            }

            return new GetAllRolesResponse { Roles = t };
        }

        public GetMyProfileEntryResponse Get(GetMyProfileEntryRequest request)
        {
            int id = 0;
            String _query = string.Format("SELECT id from {0} where eb_users_id = {1};", request.TableName, request.UserId);
            EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(_query);
            if (dt.Rows.Count > 0)
                id = Convert.ToInt32(dt.Rows[0][0]);

            return new GetMyProfileEntryResponse { RowId = id };
        }

        public GetMeetingSlotsResponse Post(GetMeetingSlotsRequest request)
        {
            GetMeetingSlotsResponse Slots = new GetMeetingSlotsResponse();
            string _qry = @"
        SELECT 
		A.id,A.max_attendees,A.Max_hosts, A.no_of_attendee, A.no_of_hosts,A.title , A.description ,A.venue, A.integration,A.duration,
		B.id as slot_id , B.eb_meeting_schedule_id,  B.is_approved, 
		B.meeting_date, B.time_from, B.time_to,
	
		COALESCE (C.slot_host, 0) as slot_host_count,
		COALESCE (C.slot_host_attendee, 0) as slot_attendee_count,
	    COALESCE (D.id, 0) as meeting_id	
		FROM	
			(SELECT 
						id, no_of_attendee, no_of_hosts , max_attendees, max_hosts, title , description , venue, integration ,duration
					FROM  
						eb_meeting_schedule 
					WHERE 
						eb_del = 'F' AND id = 1 )A
				LEFT JOIN
					(SELECT 
							id, eb_meeting_schedule_id , is_approved, 
		                        meeting_date, time_from, time_to 
	                        FROM 
		                        eb_meeting_slots 
	                        WHERE 
		                        eb_del = 'F' AND meeting_date='{1}' )B 
                        ON B.eb_meeting_schedule_id	= A.id 
                        LEFT JOIN 
                        (SELECT 
		                        eb_meeting_schedule_id,approved_slot_id ,type_of_user, COUNT(approved_slot_id)filter(where participant_type = 1) as slot_host,
						 		COUNT(approved_slot_id)filter(where participant_type = 2) as slot_host_attendee
	                        FROM 
		                        eb_meeting_slot_participants
	                        GROUP BY
		                        eb_meeting_schedule_id, approved_slot_id, type_of_user, eb_del
	                        Having
		                        eb_del = 'F')C	
                        ON
 	                        C.eb_meeting_schedule_id = A.id and C.approved_slot_id = B.id
	
                        LEFT JOIN 
                        (SELECT 
		                        id, eb_meeting_slots_id
	                        FROM 
		                        eb_meetings
	                        where
		                        eb_del = 'F') D
		                        ON
 	                        D.eb_meeting_slots_id = B.id
		ORDER BY slot_id
";
            String _query = string.Format(_qry, request.MeetingScheduleId, request.Date);
            try
            {
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(_query);
                int capacity1 = dt.Rows.Count;
                for (int i = 0; i < capacity1; i++)
                {
                    Slots.AllSlots.Add(
                        new SlotProcess()
                        {
                            Meeting_Id = Convert.ToInt32(dt.Rows[i]["id"]),
                            Slot_id = Convert.ToInt32(dt.Rows[i]["slot_id"]),
                            Meeting_schedule_id = Convert.ToInt32(dt.Rows[i]["eb_meeting_schedule_id"]),
                            Title = Convert.ToString(dt.Rows[i]["title"]),
                            Description = Convert.ToString(dt.Rows[i]["description"]),
                            Is_approved = Convert.ToString(dt.Rows[i]["is_approved"]),
                            Date = Convert.ToString(dt.Rows[i]["date"]),
                            Time_from = Convert.ToString(dt.Rows[i]["time_from"]),
                            Time_to = Convert.ToString(dt.Rows[i]["time_to"]),
                            Venue = Convert.ToString(dt.Rows[i]["venue"]),
                            Integration = Convert.ToString(dt.Rows[i]["integration"]),
                            No_Host = Convert.ToInt32(dt.Rows[i]["no_of_hosts"]),
                            No_Attendee = Convert.ToInt32(dt.Rows[i]["no_of_attendee"]),
                            Max_Attendee = Convert.ToInt32(dt.Rows[i]["max_attendees"]),
                            Max_Host = Convert.ToInt32(dt.Rows[i]["max_hosts"]),
                            SlotAttendeeCount = Convert.ToInt32(dt.Rows[i]["slot_attendee_count"]),
                            SlotHostCount = Convert.ToInt32(dt.Rows[i]["slot_participant_count"]),
                            MeetingId = Convert.ToInt32(dt.Rows[i]["slot_participant_count"]),
                            Duration = Convert.ToInt32(dt.Rows[i]["duration"]),
                        });
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message, e.StackTrace);
            }
            return Slots;
        }

        public MeetingSaveValidateResponse Post(MeetingSaveValidateRequest request)
        {
            MeetingSaveValidateResponse Resp = new MeetingSaveValidateResponse();
            string query = @" 
            SELECT 
		     A.id as slot_id , A.eb_meeting_schedule_id, A.is_approved,
			 B.no_of_attendee, B.no_of_hosts,
			 COALESCE (C.slot_host, 0) as slot_host_count,
		     COALESCE (C.slot_host_attendee, 0) as slot_attendee_count,
			 COALESCE (D.id, 0) as meeting_id,
			 COALESCE (E.id, 0) as participant_id
	            FROM
				(SELECT 
						id, eb_meeting_schedule_id , is_approved, 
					meeting_date, time_from, time_to
	                     FROM 
		                     eb_meeting_slots 
	                     WHERE 
		                     eb_del = 'F' and id = {0})A
						LEFT JOIN	 
							 (SELECT id, no_of_attendee, no_of_hosts FROM  eb_meeting_schedule)B
							 ON
 	                     B.id = A.eb_meeting_schedule_id
						LEFT JOIN	
						(SELECT 
		                     eb_meeting_schedule_id,approved_slot_id ,type_of_user, COUNT(approved_slot_id)filter(where type_of_user = 1) as slot_host,
						 		                    COUNT(approved_slot_id)filter(where type_of_user = 2) as slot_host_attendee
	                     FROM 
		                     eb_meeting_slot_participants
	                     GROUP BY
		                     eb_meeting_schedule_id, approved_slot_id, type_of_user, eb_del
	                     Having
		                     eb_del = 'F')C	
                     ON
 	                     C.eb_meeting_schedule_id = B.id and C.approved_slot_id = A.id
                     LEFT JOIN 
                     (SELECT 
		                     id, eb_meeting_slots_id
	                     FROM 
		                     eb_meetings
	                     where
		                     eb_del = 'F') D
		                     ON
 	                     D.eb_meeting_slots_id = A.id
						 LEFT JOIN (
						 SELECT id , eb_meeting_schedule_id , approved_slot_id ,type_of_user, COUNT(approved_slot_id)filter(where type_of_user = 1) as slot_host,
					COUNT(approved_slot_id)filter(where type_of_user = 2) as slot_host_attendee
	                     FROM 
		                     eb_meeting_slot_participants
							  GROUP BY
		                        eb_meeting_schedule_id, approved_slot_id, type_of_user, eb_del , id)E
								  ON
 	                     E.approved_slot_id = A.id ; 

                                        ";
            List<DetailsBySlotid> SlotObj = new List<DetailsBySlotid>();
            bool Status = false;
            try
            {
                String _query = string.Format(query, request.SlotParticipant.ApprovedSlotId); ;
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(_query);
                int capacity1 = dt.Rows.Count;
                for (int i = 0; i < capacity1; i++)
                {
                    SlotObj.Add(
                        new DetailsBySlotid()
                        {
                            Slot_id = Convert.ToInt32(dt.Rows[i]["slot_id"]),
                            Meeting_schedule_id = Convert.ToInt32(dt.Rows[i]["eb_meeting_schedule_id"]),
                            MeetingId = Convert.ToInt32(dt.Rows[i]["eb_meeting_id"]),
                            No_Attendee = Convert.ToInt32(dt.Rows[i]["no_of_attendee"]),
                            No_Host = Convert.ToInt32(dt.Rows[i]["no_of_host"]),
                            SlotHostCount = Convert.ToInt32(dt.Rows[i]["slot_host_count"]),
                            SlotAttendeeCount = Convert.ToInt32(dt.Rows[i]["slot_attendee_count"]),
                            Is_approved = Convert.ToString(dt.Rows[i]["is_approved"]),
                            Participant_id = Convert.ToInt32(dt.Rows[i]["participant_id"]),
                        });
                }
                if (request.SlotParticipant.Participant_type == 2)

                {
                    if (SlotObj[0].No_Attendee >= SlotObj[0].SlotAttendeeCount)
                    {
                        Status = true;
                    }
                }
                else
                {
                    if (SlotObj[0].No_Host >= SlotObj[0].SlotHostCount)
                    {
                        Status = true;
                    }
                }
            }
            catch (Exception e)
            {
                Status = false;
                Console.WriteLine(e.Message, e.StackTrace);
            }

            if (Status)
            {
                if (SlotObj[0].Is_approved == "T")
                {
                    query = $"insert into eb_meeting_slot_participants( user_id ,role_id ,user_group_id , confirmation , eb_meeting_schedule_id , approved_slot_id ,name ,email,phone_num," +
                            $" type_of_user,participant_type) values ({request.SlotParticipant.UserId},{request.SlotParticipant.RoleId},{request.SlotParticipant.UserGroupId},{request.SlotParticipant.Confirmation}," +
                            $"{SlotObj[0].Meeting_schedule_id},{request.SlotParticipant.ApprovedSlotId},'{request.SlotParticipant.Name}','{request.SlotParticipant.Email}','{request.SlotParticipant.PhoneNum}'," +
                            $"{request.SlotParticipant.TypeOfUser},{request.SlotParticipant.Participant_type});" +
                            $"insert into eb_meeting_participants (eb_meeting_id, eb_slot_participant_id ) values ({SlotObj[0].MeetingId} ,eb_currval('eb_meeting_slot_participants_id_seq'));";
                }
                else if (SlotObj[0].Is_approved == "F")
                {
                    query = $"insert into eb_meetings (eb_meeting_slots_id , eb_created_by)values({SlotObj[0].Slot_id}, 1);" +
                            $"insert into eb_meeting_slot_participants( user_id ,role_id ,user_group_id , confirmation , eb_meeting_schedule_id , approved_slot_id ,name ,email,phone_num," +
                            $" type_of_user,participant_type) values ({request.SlotParticipant.UserId},{request.SlotParticipant.RoleId},{request.SlotParticipant.UserGroupId},{request.SlotParticipant.Confirmation}," +
                            $"{SlotObj[0].Meeting_schedule_id},{request.SlotParticipant.ApprovedSlotId},'{request.SlotParticipant.Name}','{request.SlotParticipant.Email}','{request.SlotParticipant.PhoneNum}'," +
                            $"{request.SlotParticipant.TypeOfUser},{request.SlotParticipant.Participant_type});";
                    for (int i = 0; i < SlotObj.Count(); i++)
                    {
                        query += $"insert into eb_meeting_participants (eb_meeting_id, eb_slot_participant_id ) values ( eb_currval('eb_meetings_id_seq'),{SlotObj[i].Participant_id} );";
                    }
                    query += $"insert into eb_meeting_participants (eb_meeting_id, eb_slot_participant_id ) values (eb_currval('eb_meetings_id_seq') , eb_currval('eb_meeting_slot_participants_id_seq'));";
                    query += $"update eb_meeting_slots set is_approved = 'T' where  id ={request.SlotParticipant.ApprovedSlotId}";
                }

                try
                {
                    EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query);
                    Resp.ResponseStatus = true;
                }
                catch (Exception e)
                {
                    Resp.ResponseStatus = false;
                    Console.WriteLine(e.Message, e.StackTrace);
                }
            }
            return Resp;
        }
        public AddMeetingSlotResponse Post(AddMeetingSlotRequest request)
        {
            string qry = "";
            string date = request.Date;
            TimeSpan today = new TimeSpan(09, 00, 00);
            TimeSpan duration = new System.TimeSpan(00, 29, 00);
            TimeSpan intervals = new System.TimeSpan(00, 30, 00);
            for (int i = 0; i < 14; i++)
            {
                TimeSpan temp = today.Add(duration);
                qry += $"insert into eb_meeting_slots (eb_meeting_schedule_id,meeting_date,time_from,time_to,eb_created_by) values " +
                    $"('1','{request.Date}','{today}','{temp}', 2 );";
                today = today.Add(intervals);
            }
            try
            {
                int a = this.EbConnectionFactory.DataDB.DoNonQuery(qry);
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace, e.Message);
            }
            return new AddMeetingSlotResponse();
        }
        public SlotDetailsResponse Post(SlotDetailsRequest request)
        {
            SlotDetailsResponse Resp = new SlotDetailsResponse();
            string _qry = $@"
                 SELECT 
	            A.id,A.is_completed, B.id as slot_id,C.id as meeting_schedule_id,C.description,B.time_from,B.time_to,
				C.meeting_date ,C.venue,C.integration,C.title,
				D.user_id , D.type_of_user,D.participant_type,E.fullname
			        FROM
				   (SELECT 
						id, eb_meeting_slots_id,is_completed FROM  eb_my_actions 
	                     WHERE  eb_del = 'F' and id ={request.Id} )A
						LEFT JOIN
							 (SELECT id , eb_meeting_schedule_id,time_from,time_to FROM  eb_meeting_slots)B
							 ON B.id = A.eb_meeting_slots_id		
							 LEFT JOIN	
							 (SELECT id ,title, meeting_date,venue,integration,description FROM  eb_meeting_schedule )C
							 ON C.id = B.eb_meeting_schedule_id	
							 LEFT JOIN	
							 (SELECT id , approved_slot_id, eb_meeting_schedule_id , user_id ,type_of_user,participant_type FROM  eb_meeting_slot_participants )D
							 ON D.approved_slot_id = B.id
							 LEFT JOIN	
							 (select id, fullname from eb_users where eb_del = 'F')E
							 ON E.id = D.user_id" ;

            try
            {
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(_qry);
                int capacity1 = dt.Rows.Count;
                for (int i = 0; i < capacity1; i++)
                {
                    Resp.MeetingRequest.Add(
                        new MeetingRequest()
                        {
                            MaId = Convert.ToInt32(dt.Rows[i]["id"]),
                            Slotid = Convert.ToInt32(dt.Rows[i]["slot_id"]),
                            MaIsCompleted = Convert.ToString(dt.Rows[i]["is_completed"]),
                            MeetingScheduleid = Convert.ToInt32(dt.Rows[i]["meeting_schedule_id"]),
                            Description = Convert.ToString(dt.Rows[i]["description"]),
                            TimeFrom = Convert.ToString(dt.Rows[i]["time_from"]),
                            TimeTo = Convert.ToString(dt.Rows[i]["time_to"]),
                            Title = Convert.ToString(dt.Rows[i]["title"]),
                            MeetingDate = Convert.ToString(dt.Rows[0]["meeting_date"]),
                            Venue = Convert.ToString(dt.Rows[i]["venue"]),
                            Integration = Convert.ToString(dt.Rows[i]["integration"]),
                            fullname = Convert.ToString(dt.Rows[i]["fullname"]),
                            UserId = Convert.ToInt32(dt.Rows[i]["user_id"]),
                            TypeofUser = Convert.ToInt32(dt.Rows[i]["type_of_user"]),
                            ParticipantType = Convert.ToInt32(dt.Rows[i]["participant_type"]),
                        });
                }
            }
            catch (Exception e)
            {
                Console.WriteLine(e.StackTrace, e.Message);
            }
            return Resp;
        }

        public MeetingUpdateByUsersResponse Post(MeetingUpdateByUsersRequest request)
        {
            MeetingSaveValidateResponse Resp = new MeetingSaveValidateResponse();
            string query = @"        
            SELECT 
		     A.id as slot_id , A.eb_meeting_schedule_id, A.is_approved,
			 B.no_of_attendee, B.no_of_hosts,B.max_hosts,B.max_attendees,
			 COALESCE (D.id, 0) as meeting_id
	            FROM
				(SELECT 
						id, eb_meeting_schedule_id , is_approved, 
					meeting_date, time_from, time_to
	                     FROM 
		                     eb_meeting_slots 
	                     WHERE 
		                     eb_del = 'F' and id = {0})A
						LEFT JOIN	 
							 (SELECT id, no_of_attendee, no_of_hosts,max_hosts,max_attendees FROM  eb_meeting_schedule)B
							 ON
 	                     B.id = A.eb_meeting_schedule_id	
                     LEFT JOIN 
                     (SELECT 
		                     id, eb_meeting_slots_id
	                     FROM 
		                     eb_meetings
	                     where
		                     eb_del = 'F') D
		                     ON
 	                     D.eb_meeting_slots_id = A.id ;
                SELECT 
		     A.id as slot_id , A.eb_meeting_schedule_id,
			 COALESCE (B.id, 0) as participant_id,B.participant_type,B.type_of_user,B.user_id
	            FROM
				(SELECT id, eb_meeting_schedule_id
	                     FROM  eb_meeting_slots 
	                     WHERE  eb_del = 'F' and id = {0})A
						LEFT JOIN	
						(SELECT id, user_id,eb_meeting_schedule_id,approved_slot_id ,type_of_user,participant_type
	                     FROM eb_meeting_slot_participants
	                     GROUP BY
		                     id,user_id,eb_meeting_schedule_id, approved_slot_id, type_of_user,participant_type, eb_del
	                     Having eb_del = 'F')B
                     ON B.eb_meeting_schedule_id = A.eb_meeting_schedule_id and B.approved_slot_id = A.id 
                         where participant_type is not null; 
 
 						select count(*) as slot_attendee_count from eb_meeting_slot_participants where approved_slot_id = {0} 
									   and participant_type=2;
						select count(*) as slot_host_count from eb_meeting_slot_participants where approved_slot_id = {0} 
									   and participant_type=1;
            select id, user_ids,usergroup_id,role_ids, form_ref_id, form_data_id , description, expiry_datetime, eb_meeting_slots_id ,except_user_ids from eb_my_actions 
            where eb_meeting_slots_id = {0} and id= {1} and is_completed='F';
                                        ";

            List<MyAction> MyActionObj = new List<MyAction>();

            List<MeetingScheduleDetails> MSD = new List<MeetingScheduleDetails>(); //MSD Meeting Schedule Details
            List<SlotParticipantsDetails> SPL = new List<SlotParticipantsDetails>(); //SPL Slot Participant List
            SlotParticipantCount SPC = new SlotParticipantCount(); //SPL Slot Participant Count

            MeetingUpdateByUsersResponse resp = new MeetingUpdateByUsersResponse();
            resp.ResponseStatus = true;
            bool Status = false;
            try
            {
                String _query = string.Format(query, request.Id, request.MyActionId); ;
                EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(_query);
                for (int k = 0; k < ds.Tables[0].Rows.Count; k++)
                {
                    MSD.Add(new MeetingScheduleDetails()
                    {
                        SlotId = Convert.ToInt32(ds.Tables[0].Rows[k]["slot_id"]),
                        MeetingScheduleId = Convert.ToInt32(ds.Tables[0].Rows[k]["eb_meeting_schedule_id"]),
                        MeetingId = Convert.ToInt32(ds.Tables[0].Rows[k]["meeting_id"]),
                        MinAttendees = Convert.ToInt32(ds.Tables[0].Rows[k]["no_of_attendee"]),
                        MinHosts = Convert.ToInt32(ds.Tables[0].Rows[k]["no_of_hosts"]),
                        MaxAttendees = Convert.ToInt32(ds.Tables[0].Rows[k]["max_attendees"]),
                        MaxHosts = Convert.ToInt32(ds.Tables[0].Rows[k]["max_hosts"]),
                        IsApproved = Convert.ToString(ds.Tables[0].Rows[k]["is_approved"])
                    });
                }
                for (int k = 0; k < ds.Tables[1].Rows.Count; k++)
                {
                    SPL.Add(new SlotParticipantsDetails()
                    {
                        SlotId = Convert.ToInt32(ds.Tables[1].Rows[k]["slot_id"]),
                        MeetingScheduleId = Convert.ToInt32(ds.Tables[1].Rows[k]["eb_meeting_schedule_id"]),
                        ParticipantId = Convert.ToInt32(ds.Tables[1].Rows[k]["participant_id"]),
                        ParticipantType = Convert.ToInt32(ds.Tables[1].Rows[k]["participant_type"]),
                        TypeOfUser = Convert.ToInt32(ds.Tables[1].Rows[k]["type_of_user"]),
                        UserId = Convert.ToInt32(ds.Tables[1].Rows[k]["user_id"]),
                    });
                }
                SPC.SlotAttendeeCount = Convert.ToInt32(ds.Tables[2].Rows[0]["slot_attendee_count"]);
                SPC.SlotHostCount = Convert.ToInt32(ds.Tables[3].Rows[0]["slot_host_count"]);
                for (int i = 0; i < ds.Tables[4].Rows.Count; i++)
                {
                    MyActionObj.Add(new MyAction()
                    {
                        Id = Convert.ToInt32(ds.Tables[4].Rows[i]["id"]),
                        SlotId = Convert.ToInt32(ds.Tables[4].Rows[i]["eb_meeting_slots_id"]),
                        Description = Convert.ToString(ds.Tables[4].Rows[i]["description"]),
                        UserIds = Convert.ToString(ds.Tables[4].Rows[i]["user_ids"]),
                        RoleIds = Convert.ToString(ds.Tables[4].Rows[i]["role_ids"]),
                        FormRefId = Convert.ToString(ds.Tables[4].Rows[i]["form_ref_id"]),
                        ExpiryDateTime = Convert.ToString(ds.Tables[4].Rows[i]["expiry_datetime"]),
                        ExceptUserIds = Convert.ToString(ds.Tables[4].Rows[i]["except_user_ids"]),
                        UserGroupId = Convert.ToInt32(ds.Tables[4].Rows[i]["usergroup_id"]),
                        FormDataId = Convert.ToInt32(ds.Tables[4].Rows[i]["form_data_id"]),

                    });
                }
            }
            catch (Exception e)
            {
                Status = false;
                Console.WriteLine(e.Message, e.StackTrace);
            }
            string qry_ = "";
            if (MSD[0].MaxHosts > SPC.SlotHostCount && MyActionObj.Count != 0)
            {
                if (MSD[0].IsApproved=="F" && MSD[0].MinHosts == (SPC.SlotHostCount + 1) && MSD[0].MinAttendees <= SPC.SlotAttendeeCount && MSD[0].MaxAttendees >= SPC.SlotAttendeeCount)
                {
                    qry_ += $@"insert into eb_meetings (eb_meeting_slots_id, eb_created_at, eb_created_by) values({MSD[0].SlotId}, now(), {request.UserInfo.UserId});
                        insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.UserId}, 1, {MSD[0].MeetingScheduleId}, {request.Id}, '{request.UserInfo.FullName}', '{request.UserInfo.Email}', 1, 1); ";
                    for (int k = 0; k < SPL.Count; k++)
                        qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id) values ( eb_currval('eb_meetings_id_seq'),{SPL[k].ParticipantId}); ";

                    qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id ) values (eb_currval('eb_meetings_id_seq'), eb_currval('eb_meeting_slot_participants_id_seq'));";
                    qry_ += $"update eb_meeting_slots set is_approved = 'T' where  id = {request.Id}; ";
                }
                else if (MSD[0].IsApproved == "T" && MSD[0].MeetingId > 0)
                {
                    qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.Id}, 1, {MSD[0].MeetingScheduleId}, {request.Id}, '{request.UserInfo.FullName}', '', 1, 1); ";
                    qry_ += $"insert into eb_meeting_participants(eb_meeting_id, eb_slot_participant_id ) values ({MSD[0].MeetingId}, eb_currval('eb_meeting_slot_participants_id_seq'));";
                }
                else if (MSD[0].IsApproved == "F" && MSD[0].MinHosts > (SPC.SlotHostCount + 1))
                {
                    qry_ += $@"insert into eb_meeting_slot_participants(user_id, confirmation, eb_meeting_schedule_id, approved_slot_id, name, email, type_of_user, participant_type) 
                            values ({request.UserInfo.Id}, 1, {MSD[0].MeetingScheduleId}, {request.Id}, '{request.UserInfo.FullName}', '', 1, 1); ";
                }

                if (MSD[0].MaxHosts == (SPC.SlotHostCount + 1))
                {
                    qry_ += $"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where eb_meeting_slots_id = {request.Id} " +
                        $"and id= {request.MyActionId}; ";
                }
                else
                {
                    qry_ += $"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where eb_meeting_slots_id = {request.Id} " +
                       $"and id= {request.MyActionId};";
                    qry_ += $@"insert into eb_my_actions (user_ids,usergroup_id,role_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_slots_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].UserIds}',{MyActionObj[0].UserGroupId},'{MyActionObj[0].RoleIds}',
                        NOW(),{MyActionObj[0].FormRefId}, {request.Id}, {MyActionObj[0].Description},'{MyActionTypes.Meeting}',
                        {MyActionObj[0].Description} , 'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";
                }
               
            }
            else
            {

            }
            try
            {
                int a = this.EbConnectionFactory.DataDB.DoNonQuery(qry_);
            }
            catch (Exception e)
            {
                resp.ResponseStatus = false;
                Console.WriteLine(e.StackTrace, e.Message);
            }
            return resp;
        }

        public MeetingCancelByHostResponse Post(MeetingCancelByHostRequest request)
        {
            string query = @"
             select id, user_ids,usergroup_id,role_ids, form_ref_id, form_data_id , description, expiry_datetime, eb_meeting_slots_id ,except_user_ids from eb_my_actions 
            where eb_meeting_slots_id = {0} and id= {1} and is_completed='F';
                                        ";
            List<MyAction> MyActionObj = new List<MyAction>();
            MeetingCancelByHostResponse Resp = new MeetingCancelByHostResponse();
            string qry_ = "";
            try
            {
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query);
                int capacity1 = dt.Rows.Count;
                for (int i = 0; i < capacity1; i++)
                {
                    MyActionObj.Add(new MyAction()
                    {
                        Id = Convert.ToInt32(dt.Rows[i]["id"]),
                        SlotId = Convert.ToInt32(dt.Rows[i]["eb_meeting_slots_id"]),
                        Description = Convert.ToString(dt.Rows[i]["description"]),
                        UserIds = Convert.ToString(dt.Rows[i]["user_ids"]),
                        RoleIds = Convert.ToString(dt.Rows[i]["role_ids"]),
                        FormRefId = Convert.ToString(dt.Rows[i]["form_ref_id"]),
                        ExpiryDateTime = Convert.ToString(dt.Rows[i]["expiry_datetime"]),
                        ExceptUserIds = Convert.ToString(dt.Rows[i]["except_user_ids"]),
                        UserGroupId = Convert.ToInt32(dt.Rows[i]["usergroup_id"]),
                        FormDataId = Convert.ToInt32(dt.Rows[i]["form_data_id"]),
                    });
                 }
                qry_ += $"update eb_my_actions set completed_at = now(), completed_by ={request.UserInfo.UserId} , is_completed='T' where eb_meeting_slots_id = {request.SlotId} " +
                      $"and id= {request.MyActionId};";
                qry_ += $@"insert into eb_my_actions (user_ids,usergroup_id,role_ids,from_datetime,form_ref_id,form_data_id,description,my_action_type , eb_meeting_slots_id,
                        is_completed,eb_del , except_user_ids)
                        values('{MyActionObj[0].UserIds}',{MyActionObj[0].UserGroupId},'{MyActionObj[0].RoleIds}',
                        NOW(),{MyActionObj[0].FormRefId}, {request.SlotId}, {MyActionObj[0].Description},'{MyActionTypes.Meeting}',
                        {MyActionObj[0].Description} , 'F','F' ,'{request.UserInfo.UserId},{MyActionObj[0].ExceptUserIds}');";

                    int a = this.EbConnectionFactory.DataDB.DoNonQuery(qry_);
                Resp.ResponseStatus = true;
             
            }
            catch (Exception e)
            {
                Resp.ResponseStatus = false;
                Console.WriteLine(e.StackTrace, e.Message);
            }
            return Resp;
        }
        public MeetingRejectByHostResponse Post(MeetingRejectByHostRequest request)
        {

            return new MeetingRejectByHostResponse();
        }
        //public MeetingRequestByEligibleAttendeeResponse(MeetingRequestByEligibleAttendeeRequest request)
        //{

        //}
    }
}

//        public UpdateSlotParticipantResponse Post(UpdateSlotParticipantRequest request)
//        {
//            UpdateSlotParticipantResponse Resp = new UpdateSlotParticipantResponse();
//            Resp.ResponseStatus = true;
//            string query = "";
//            if (request.SlotInfo.Is_approved == "T")
//            {
//                query = $"insert into eb_meeting_participants (eb_meeting_id, eb_slot_participant_id ) values ({request.SlotInfo.MeetingId} ," +
//              $"insert into eb_meeting_slot_participants( user_id ,role_id ,user_group_id , confirmation , eb_meeting_schedule_id , approved_slot_id ,name ,email,phone_num," +
//              $" type_of_user,participant_type) values ({request.SlotParticipant.UserId},{request.SlotParticipant.RoleId},{request.SlotParticipant.UserGroupId},{request.SlotParticipant.Confirmation}," +
//              $"{request.SlotInfo.Meeting_schedule_id},{request.SlotParticipant.ApprovedSlotId},'{request.SlotParticipant.Name}','{request.SlotParticipant.Email}','{request.SlotParticipant.PhoneNum}'," +
//              $"{request.SlotParticipant.TypeOfUser},{request.SlotParticipant.Participant_type}));";
//            }
//            else if (request.SlotInfo.Is_approved == "F")
//            {
//                query = $"insert into eb_meeting_slot_participants( user_id ,role_id ,user_group_id , confirmation , eb_meeting_schedule_id , approved_slot_id ,name ,email,phone_num," +
//                                $" type_of_user,participant_type) values ({request.SlotParticipant.UserId},{request.SlotParticipant.RoleId},{request.SlotParticipant.UserGroupId},{request.SlotParticipant.Confirmation}," +
//                                $"{request.SlotInfo.Meeting_schedule_id},{request.SlotParticipant.ApprovedSlotId},'{request.SlotParticipant.Name}','{request.SlotParticipant.Email}','{request.SlotParticipant.PhoneNum}'," +
//                                $"{request.SlotParticipant.TypeOfUser},{request.SlotParticipant.Participant_type})";
//            }

//            try
//            {
//                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(query);
//            }
//            catch (Exception e)
//            {
//                Resp.ResponseStatus = false;
//                Console.WriteLine(e.Message, e.StackTrace);
//            }
//            return Resp;
//        }
//    }
//}
