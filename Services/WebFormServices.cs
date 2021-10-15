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
using System.Net;
using System.Globalization;

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
                Form.AfterRedisGet_All(this);
                if (Form.DataPushers.Count > 0)
                {
                    foreach (EbDataPusher pusher in Form.DataPushers)
                    {
                        if (pusher is EbApiDataPusher)
                            continue;
                        EbWebForm _form = this.GetWebFormObject(pusher.FormRefId, null, null);
                        ExpressBase.Common.TableSchema _table = _form.FormSchema.Tables.Find(e => e.TableName.Equals(_form.FormSchema.MasterTable));
                        //_table.Columns.Add(new ColumnSchema { ColumnName = "eb_push_id", EbDbType = (int)EbDbTypes.String, Control = new EbTextBox { Name = "eb_push_id", Label = "Push Id" } });// multi push id
                        //_table.Columns.Add(new ColumnSchema { ColumnName = "eb_src_id", EbDbType = (int)EbDbTypes.Decimal, Control = new EbNumeric { Name = "eb_src_id", Label = "Source Id" } });// source master table id
                        if (_table != null)
                        {
                            if (pusher is EbBatchFormDataPusher batchDp)
                            {
                                ExpressBase.Common.TableSchema __tbl = Form.FormSchema.Tables.Find(e => e.ContainerName == batchDp.SourceDG);
                                if (__tbl != null)
                                    _table.Columns.Add(new ColumnSchema
                                    {
                                        ColumnName = __tbl.TableName + FormConstants._id,
                                        EbDbType = (int)EbDbTypes.Int32,
                                        Control = new EbNumeric { Name = __tbl.TableName + FormConstants._id }
                                    });

                            }

                            Form.FormSchema.Tables.Add(_table);
                        }
                    }
                }
                CreateWebFormTables(Form, request);
                InsertDataIfRequired(Form.FormSchema, Form.RefId);
            }
            return new CreateWebFormTableResponse { };
        }

        public CreateMyProfileTableResponse Any(CreateMyProfileTableRequest request)
        {
            List<TableColumnMeta> listNamesAndTypes = new List<TableColumnMeta>
            {
                new TableColumnMeta { Name = "eb_users_id", Type = this.EbConnectionFactory.DataDB.VendorDbTypes.Int32 }
            };
            if (request.UserTypeForms != null)
            {
                foreach (EbProfileUserType eput in request.UserTypeForms)
                {
                    if (string.IsNullOrEmpty(eput.RefId))
                        continue;
                    EbWebForm _form = EbFormHelper.GetEbObject<EbWebForm>(eput.RefId, null, this.Redis, this);
                    string Msg = string.Empty;
                    CreateOrAlterTable(_form.TableName, listNamesAndTypes, ref Msg);
                    Console.WriteLine("CreateMyProfileTableRequest - WebForm Resp msg: " + Msg);
                }
            }
            if (request.UserTypeMobPages != null)
            {
                foreach (EbProfileUserType eput in request.UserTypeMobPages)
                {
                    if (string.IsNullOrEmpty(eput.RefId))
                        continue;

                    EbMobilePage _mobPage = EbFormHelper.GetEbObject<EbMobilePage>(eput.RefId, null, this.Redis, this);
                    if (!(_mobPage.Container is EbMobileForm))
                        continue;
                    string Msg = string.Empty;
                    CreateOrAlterTable((_mobPage.Container as EbMobileForm).TableName, listNamesAndTypes, ref Msg);
                    Console.WriteLine("CreateMyProfileTableRequest - MobileForm Resp msg: " + Msg);
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

        private void CreateWebFormTables(EbWebForm Form, CreateWebFormTableRequest request)
        {
            WebFormSchema _schema = Form.FormSchema;
            Form.SolutionObj = request.SoluObj ?? this.GetSolutionObject(request.SolnId);
            EbSystemColumns ebs = Form.SolutionObj.SolutionSettings?.SystemColumns ?? new EbSystemColumns(EbSysCols.Values);// Solu Obj is null
            IVendorDbTypes vDbTypes = this.EbConnectionFactory.DataDB.VendorDbTypes;
            string Msg = string.Empty;
            foreach (ExpressBase.Common.TableSchema _table in _schema.Tables)
            {
                List<TableColumnMeta> _listNamesAndTypes = new List<TableColumnMeta>();
                if (_table.Columns.Count > 0 && _table.TableType != WebFormTableTypes.Review)
                {
                    bool CurrencyCtrlFound = false;
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
                        {
                            _listNamesAndTypes.Add(new TableColumnMeta { Name = _column.ColumnName, Type = vDbTypes.GetVendorDbTypeStruct((EbDbTypes)_column.EbDbType), Label = _column.Control.Label, Control = _column.Control });
                            if (_column.Control is EbPhone && (_column.Control as EbPhone).Sendotp)
                                _listNamesAndTypes.Add(new TableColumnMeta { Name = _column.ColumnName + FormConstants._verified, Type = vDbTypes.Boolean, Default = "F", Label = _column.Control.Label + "_verified" });

                            if ((_column.Control is EbNumeric numCtrl && numCtrl.InputMode == NumInpMode.Currency) ||
                                (_column.Control is EbDGNumericColumn numCol && numCol.InputMode == NumInpMode.Currency))
                                CurrencyCtrlFound = true;
                        }
                    }
                    if (_table.TableName == _schema.MasterTable)
                    {
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_ver_id], Type = vDbTypes.Int32 });// id refernce to the parent table will store in this column - foreignkey
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_lock], Type = vDbTypes.GetVendorDbTypeStruct(ebs.GetDbType(SystemColumns.eb_lock)), Default = ebs.GetBoolFalse(SystemColumns.eb_lock, false), Label = "Lock ?" });// lock to prevent editing
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_push_id], Type = vDbTypes.String, Label = "Multi push id" });// multi push id - for data pushers
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_src_id], Type = vDbTypes.Int32, Label = "Source id" });// source id - for data pushers
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_src_ver_id], Type = vDbTypes.Int32, Label = "Source version id" });// source version id - for data pushers
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_ro], Type = vDbTypes.GetVendorDbTypeStruct(ebs.GetDbType(SystemColumns.eb_ro)), Default = ebs.GetBoolFalse(SystemColumns.eb_ro, false), Label = "Read Only?" });// Readonly
                    }
                    else
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = _schema.MasterTable + "_id", Type = vDbTypes.Int32 });// id refernce to the parent table will store in this column - foreignkey
                    if (_table.TableType == WebFormTableTypes.Grid)
                    {
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_row_num], Type = vDbTypes.Int32 });// data grid row number
                        if (_table.IsDynamic)// if data grid is in dynamic tab then adding column for source reference - foreignkey
                        {
                            foreach (ExpressBase.Common.TableSchema _t in _schema.Tables.FindAll(e => e.TableType == WebFormTableTypes.Grid && e != _table))
                                _listNamesAndTypes.Add(new TableColumnMeta { Name = _t.TableName + "_id", Type = vDbTypes.Int32 });
                        }
                    }

                    _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_created_by], Type = vDbTypes.Int32, Label = "Created By" });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_created_at], Type = vDbTypes.DateTime, Label = "Created At" });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_lastmodified_by], Type = vDbTypes.Int32, Label = "Last Modified By" });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_lastmodified_at], Type = vDbTypes.DateTime, Label = "Last Modified At" });
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_del], Type = vDbTypes.GetVendorDbTypeStruct(ebs.GetDbType(SystemColumns.eb_del)), Default = ebs.GetBoolFalse(SystemColumns.eb_del, false) });// delete
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_void], Type = vDbTypes.GetVendorDbTypeStruct(ebs.GetDbType(SystemColumns.eb_void)), Default = ebs.GetBoolFalse(SystemColumns.eb_void, false), Label = "Void ?" });// cancel //only ?
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_loc_id], Type = vDbTypes.Int32, Label = "Location" });// location id //only ?
                    _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_signin_log_id], Type = vDbTypes.Int32, Label = "Log Id" });
                    //_listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_default", Type = vDbTypes.Boolean, Default = "F" });

                    if (CurrencyCtrlFound)
                    {
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_currency_id], Type = vDbTypes.Int32, Label = "Currency Id" });
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_currency_xid], Type = vDbTypes.String, Label = "Currency Xid" });
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_xrate1], Type = vDbTypes.Decimal, Label = "Xrate1" });
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = ebs[SystemColumns.eb_xrate2], Type = vDbTypes.Decimal, Label = "Xrate1" });
                    }

                    int _rowaff = CreateOrAlterTable(_table.TableName, _listNamesAndTypes, ref Msg);
                    if (_table.TableName == _schema.MasterTable && !request.IsImport && (request.WebObj as EbWebForm).AutoDeployTV)
                    {
                        if (_schema.ExtendedControls.Find(e => e is EbReview) != null)
                            _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_approval", Label = "Approval" });
                        if (!request.IsImport)
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
                                (entry.Type.EbDbType.ToString().Equals("Int32") && dr.Type.ToString().Equals("Decimal")) ||
                                (entry.Type.EbDbType.ToString().Equals("DateTime") && dr.Type.ToString().Equals("Date")) ||
                                (entry.Type.EbDbType.ToString().Equals("Date") && dr.Type.ToString().Equals("DateTime")) ||
                                (entry.Type.EbDbType.ToString().Equals("Time") && dr.Type.ToString().Equals("DateTime"))
                                ))
                                Msg += $"Type mismatch found '{dr.Type}' instead of '{entry.Type.EbDbType}' for {tableName}.{entry.Name}; ";
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
                dv = EbFormHelper.GetEbObject<EbTableVisualization>(AutogenId, null, Redis, this);
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
                DisplayName = obj.DisplayName,
                SolnId = request.SolnId
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
                    {
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
                        if (_RenderType == EbDbTypes.Time)
                            (_col as DVDateTimeColumn).Format = DateFormat.Time;
                        else if (_RenderType == EbDbTypes.DateTime)
                            (_col as DVDateTimeColumn).Format = DateFormat.DateTime;
                    }

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
                    {
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
                        if (_RenderType == EbDbTypes.Time)
                            (_col as DVDateTimeColumn).Format = DateFormat.Time;
                        else if (_RenderType == EbDbTypes.DateTime)
                            (_col as DVDateTimeColumn).Format = DateFormat.DateTime;
                    }

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
                        {
                            if (_col.RenderType == EbDbTypes.Time)
                                (_col as DVDateTimeColumn).Format = DateFormat.Time;
                            else if (_col.RenderType == EbDbTypes.DateTime)
                                (_col as DVDateTimeColumn).Format = DateFormat.DateTime;
                            _col.RenderType = column.Type.EbDbType;
                            _col.Type = column.Type.EbDbType;
                        }

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
                EbWebForm form = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId, request.CurrentLoc);
                form.TableRowId = request.RowId;
                if (form.TableRowId > 0)
                    form.RefreshFormData(EbConnectionFactory.DataDB, this);
                else
                {
                    //if (form.UserObj.LocationIds.Contains(-1) || form.UserObj.LocationIds.Contains(request.CurrentLoc))
                    //{
                    //    if (form.SolutionObj.Locations.ContainsKey(request.CurrentLoc))
                    //        form.UserObj.Preference.DefaultLocation = request.CurrentLoc;
                    //}
                    form.FormData = form.GetEmptyModel();
                }
                //if (form.SolutionObj.SolutionSettings != null && form.SolutionObj.SolutionSettings.SignupFormRefid != string.Empty && form.SolutionObj.SolutionSettings.SignupFormRefid == form.RefId)
                //{
                //}
                //else if (form.SolutionObj.SolutionSettings != null && form.SolutionObj.SolutionSettings.UserTypeForms != null && form.SolutionObj.SolutionSettings.UserTypeForms.Any(x => x.RefId == form.RefId))
                //{
                //}
                //bot c
                //else if (!(form.HasPermission(OperationConstants.VIEW, request.CurrentLoc) || form.HasPermission(OperationConstants.NEW, request.CurrentLoc) || form.HasPermission(OperationConstants.EDIT, request.CurrentLoc)))
                //{
                //    throw new FormException("Error in loading data. Access Denied.", (int)HttpStatusCodes.UNAUTHORIZED, "Access Denied for rowid " + form.TableRowId + " , current location " + form.LocationId, string.Empty);
                //}
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper()
                {
                    FormData = form.FormData,
                    Status = (int)HttpStatusCode.OK,
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
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper() { Message = "Something went wrong", Status = (int)HttpStatusCode.InternalServerError, MessageInt = ex.Message, StackTraceInt = ex.StackTrace });
            }
            return _dataset;
        }

        public GetPrefillDataResponse Any(GetPrefillDataRequest request)
        {
            Console.WriteLine("Start GetPrefillData");
            GetPrefillDataResponse _dataset = new GetPrefillDataResponse();
            try
            {
                EbWebForm form = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId, request.CurrentLoc);
                form.TableRowId = 0;
                form.RefreshFormData(EbConnectionFactory.DataDB, this, request.Params);
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper { FormData = form.FormData, Status = (int)HttpStatusCode.OK, Message = "Success" });
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
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper { Message = "Something went wrong.", Status = (int)HttpStatusCode.InternalServerError, MessageInt = e.Message, StackTraceInt = e.StackTrace });
            }
            return _dataset;
        }

        public GetExportFormDataResponse Any(GetExportFormDataRequest request)
        {
            Console.WriteLine("Start GetExportFormData");
            GetExportFormDataResponse _dataset = new GetExportFormDataResponse();
            try
            {
                EbWebForm sourceForm = this.GetWebFormObject(request.SourceRefId, request.UserAuthId, request.SolnId, request.CurrentLoc);
                sourceForm.TableRowId = request.SourceRowId;

                EbWebForm destForm;
                if (request.SourceRefId == request.DestRefId)
                {
                    destForm = sourceForm;
                    if (request.SourceRowId > 0)
                    {
                        sourceForm.RefreshFormData(EbConnectionFactory.DataDB, this);
                        destForm.FormData = EbFormHelper.GetFilledNewFormData(sourceForm, true);
                    }
                    else
                        destForm.FormData = destForm.GetEmptyModel();
                }
                else
                {
                    destForm = this.GetWebFormObject(request.DestRefId, null, null, request.CurrentLoc);
                    destForm.UserObj = sourceForm.UserObj;
                    destForm.SolutionObj = sourceForm.SolutionObj;
                    if (request.SourceRowId > 0)
                    {
                        sourceForm.RefreshFormData(EbConnectionFactory.DataDB, this);
                        sourceForm.FormatImportData(EbConnectionFactory.DataDB, this, destForm);
                    }
                    else
                        destForm.FormData = destForm.GetEmptyModel();
                }

                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper { FormData = destForm.FormData, Status = (int)HttpStatusCode.OK, Message = "Success" });
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
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper { Message = "Something went wrong.", Status = (int)HttpStatusCode.InternalServerError, MessageInt = e.Message, StackTraceInt = e.StackTrace });
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
                resp = new GetFormData4MobileResponse() { Params = data, Status = (int)HttpStatusCode.OK, Message = "Success" };
                Console.WriteLine("End GetFormData4Mobile : Success");
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in GetFormData4Mobile Service \nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace : " + ex.StackTrace);
                resp = new GetFormData4MobileResponse() { Status = (int)HttpStatusCode.InternalServerError, Message = $"{ex.Message} {ex.MessageInternal}" };
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception in GetFormData4Mobile Service \nMessage : " + e.Message + "\nStackTrace : " + e.StackTrace);
                resp = new GetFormData4MobileResponse() { Status = (int)HttpStatusCode.InternalServerError, Message = $"{e.Message} {e.StackTrace}" };
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
                if (request.Type == ImportDataType.PowerSelect)
                {
                    WebformData _FormData = JsonConvert.DeserializeObject<WebformData>(request.WebFormData);
                    if (!(_FormData?.MultipleTables?.ContainsKey(form.FormSchema.MasterTable) == true &&
                        _FormData.MultipleTables[form.FormSchema.MasterTable].Count > 0))
                        throw new FormException("Bad request", (int)HttpStatusCode.BadRequest, "WebFormData in request does not contains master table.", "WebFormService->GetImportDataRequest");
                    form.FormDataBackup = _FormData;
                    form.PsImportData(EbConnectionFactory.DataDB, this, request.Trigger);
                }
                else
                {
                    form.ImportData(EbConnectionFactory.DataDB, this, request.Params, request.Trigger, request.RowId);
                }
                data = new WebformDataWrapper { FormData = form.FormData, Status = (int)HttpStatusCode.OK, Message = "Success" };
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
                data = new WebformDataWrapper { Status = (int)HttpStatusCode.InternalServerError, Message = "Exception in GetImportDataRequest", MessageInt = ex.Message, StackTraceInt = ex.StackTrace };
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
                data = new WebformDataWrapper { FormData = wfd, Status = (int)HttpStatusCode.OK, Message = "Success" };
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
                data = new WebformDataWrapper { Status = (int)HttpStatusCode.InternalServerError, Message = "Exception in GetDynamicGridDataRequest", MessageInt = ex.Message, StackTraceInt = ex.StackTrace };
            }
            return new GetDynamicGridDataResponse() { FormDataWrap = JsonConvert.SerializeObject(data) };
        }

        public ExecuteSqlValueExprResponse Any(ExecuteSqlValueExprRequest request)
        {
            Console.WriteLine("Start ExecuteSqlValueExpr");
            EbWebForm form = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);
            string val = form.ExecuteSqlValueExpression(EbConnectionFactory.DataDB, this, request.Params, request.Trigger, request.ExprType);
            Console.WriteLine("End ExecuteSqlValueExpr");
            return new ExecuteSqlValueExprResponse() { Result = val };
        }

        public GetDataPusherJsonResponse Any(GetDataPusherJsonRequest request)
        {
            Console.WriteLine("Start GetDataPusherJson");
            EbWebForm form = this.GetWebFormObject(request.RefId, null, null);
            string val = form.GetDataPusherJson();
            Console.WriteLine("End GetDataPusherJson");
            return new GetDataPusherJsonResponse() { Json = val };
        }


        private EbWebForm GetWebFormObject(string RefId, string UserAuthId, string SolnId, int CurrrentLocation = 0)
        {
            EbWebForm _form = EbFormHelper.GetEbObject<EbWebForm>(RefId, null, this.Redis, this);
            _form.LocationId = CurrrentLocation;
            _form.SetRedisClient(this.Redis);
            if (UserAuthId != null)
            {
                _form.UserObj = GetUserObject(UserAuthId);
                if (_form.UserObj == null)
                    throw new Exception("User Object is null. AuthId: " + UserAuthId);
                if (_form.UserObj.Preference != null)
                    _form.UserObj.Preference.CurrrentLocation = CurrrentLocation;
            }
            if (SolnId != null)
            {
                _form.SolutionObj = this.GetSolutionObject(SolnId);
                if (_form.SolutionObj == null)
                    throw new Exception("Solution Object is null. SolnId: " + SolnId);
                if (_form.SolutionObj.SolutionSettings == null)
                    _form.SolutionObj.SolutionSettings = new SolutionSettings() { SystemColumns = new EbSystemColumns(EbSysCols.Values) };
                else if (_form.SolutionObj.SolutionSettings.SystemColumns == null)
                    _form.SolutionObj.SolutionSettings.SystemColumns = new EbSystemColumns(EbSysCols.Values);
            }
            _form.AfterRedisGet_All(this);
            return _form;
        }

        public DoUniqueCheckResponse Any(DoUniqueCheckRequest Req)
        {
            string fullQuery = string.Empty;
            List<DbParameter> Dbparams = new List<DbParameter>();
            Dictionary<string, bool> resp = new Dictionary<string, bool>();
            Eb_Solution SoluObj = this.GetSolutionObject(Req.SolnId);
            EbSystemColumns SysCols = SoluObj.SolutionSettings?.SystemColumns;
            if (SysCols == null)
                SysCols = new EbSystemColumns(EbSysCols.Values);

            for (int i = 0; i < Req.UniqCheckParam.Length; i++)
            {
                EbDbTypes _type = (EbDbTypes)Req.UniqCheckParam[i].TypeI;
                fullQuery += string.Format("SELECT id FROM {0} WHERE {5}{1}{6} = {5}@value_{2}{6} AND COALESCE({3}, {4}) = {4};",
                    Req.UniqCheckParam[i].TableName,
                    Req.UniqCheckParam[i].Field,
                    i,
                    SysCols[SystemColumns.eb_del],
                    SysCols.GetBoolFalse(SystemColumns.eb_del),
                    _type == EbDbTypes.String ? "LOWER(TRIM(" : string.Empty,
                    _type == EbDbTypes.String ? "))" : string.Empty);
                Dbparams.Add(this.EbConnectionFactory.DataDB.GetNewParameter("value_" + i, _type, Req.UniqCheckParam[i].Value));
            }

            if (fullQuery != string.Empty)
            {
                EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(fullQuery, Dbparams.ToArray());
                for (int i = 0; i < ds.Tables.Count; i++)
                {
                    if (ds.Tables[i].Rows.Count > 0)
                        resp.Add(Req.UniqCheckParam[i].Field, false);
                    else
                        resp.Add(Req.UniqCheckParam[i].Field, true);
                }
            }
            return new DoUniqueCheckResponse { Response = resp };
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

        //Normal save
        public InsertDataFromWebformResponse Any(InsertDataFromWebformRequest request)
        {
            try
            {
                Dictionary<string, string> MetaData = new Dictionary<string, string>();
                DateTime startdt = DateTime.Now;
                Console.WriteLine("Insert/Update WebFormData : start - " + startdt);
                EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId, request.CurrentLoc);
                CheckDataPusherCompatibility(FormObj);
                FormObj.TableRowId = request.RowId;
                FormObj.FormData = JsonConvert.DeserializeObject<WebformData>(request.FormData);
                FormObj.DraftId = request.DraftId;
                CheckForMyProfileForms(FormObj, request.WhichConsole, request.MobilePageRefId);

                Console.WriteLine("Insert/Update WebFormData : MergeFormData start - " + DateTime.Now);
                FormObj.MergeFormData();
                Console.WriteLine("Insert/Update WebFormData : Save start - " + DateTime.Now);
                string r = FormObj.Save(EbConnectionFactory, this, request.WhichConsole);
                Console.WriteLine("Insert/Update WebFormData : AfterExecutionIfUserCreated start - " + DateTime.Now);
                FormObj.AfterExecutionIfUserCreated(this, this.EbConnectionFactory.EmailConnection, MessageProducer3, request.WhichConsole, MetaData);
                Console.WriteLine("Insert/Update WebFormData end : Execution Time = " + (DateTime.Now - startdt).TotalMilliseconds);

                return new InsertDataFromWebformResponse()
                {
                    Message = "Success",
                    RowId = FormObj.TableRowId,
                    FormData = JsonConvert.SerializeObject(FormObj.FormData),
                    RowAffected = 1,
                    AffectedEntries = r,
                    Status = (int)HttpStatusCode.OK,
                    MetaData = MetaData
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
                    Status = (int)HttpStatusCode.InternalServerError,
                    MessageInt = ex.Message,
                    StackTraceInt = ex.StackTrace
                };
            }
        }

        public ExecuteReviewResponse Any(ExecuteReviewRequest request)
        {
            try
            {
                DateTime startdt = DateTime.Now;
                Console.WriteLine("ExecuteReviewRequest : start - " + startdt);
                EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId, request.CurrentLoc);
                FormObj.TableRowId = request.RowId;
                FormObj.FormData = JsonConvert.DeserializeObject<WebformData>(request.FormData);
                FormObj.DraftId = request.DraftId;
                FormObj.FormDataPusherCount = 0;// DataPusher exec blocker 
                FormObj.FormCollection = new EbWebFormCollection(FormObj);
                EbReviewHelper.CheckReviewCompatibility(FormObj);
                Console.WriteLine("ExecuteReviewRequest : MergeFormData start - " + DateTime.Now);
                FormObj.MergeFormData();
                Console.WriteLine("ExecuteReviewRequest : Save start - " + DateTime.Now);
                string r = FormObj.SaveReview(EbConnectionFactory, this, request.WhichConsole);
                Console.WriteLine("ExecuteReviewRequest end : Execution Time = " + (DateTime.Now - startdt).TotalMilliseconds);

                return new ExecuteReviewResponse()
                {
                    Message = "Success",
                    RowId = FormObj.TableRowId,
                    FormData = JsonConvert.SerializeObject(FormObj.FormData),
                    RowAffected = 1,
                    Status = (int)HttpStatusCode.OK
                };
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in ExecuteReview\nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace" + ex.StackTrace);
                return new ExecuteReviewResponse()
                {
                    Message = ex.Message,
                    Status = ex.ExceptionCode,
                    MessageInt = ex.MessageInternal,
                    StackTraceInt = ex.StackTraceInternal
                };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in ExecuteReview\nMessage : " + ex.Message + "\nStackTrace : " + ex.StackTrace);
                return new ExecuteReviewResponse()
                {
                    Message = "Something went wrong",
                    Status = (int)HttpStatusCode.InternalServerError,
                    MessageInt = ex.Message,
                    StackTraceInt = ex.StackTrace
                };
            }
        }

        private void CheckDataPusherCompatibility(EbWebForm FormObj)
        {
            if (FormObj.DataPushers != null && FormObj.DataPushers.Exists(e => !(e is EbFormDataPusher || e is EbApiDataPusher || e is EbBatchFormDataPusher)))
                throw new FormException("DataPusher config is invalid! Contact Admin.", (int)HttpStatusCode.InternalServerError, "Check the type of all DataPushers. [Save the form in dev side]", "WebFormService -> CheckDataPusherCompatibility");
        }

        private void CheckForMyProfileForms(EbWebForm FormObj, string WC, string MobilePageRefId)
        {
            if (WC == TokenConstants.UC)
            {
                if (FormObj.SolutionObj?.SolutionSettings?.UserTypeForms == null)
                    return;
                CheckForMyProfileForms(FormObj, FormObj.SolutionObj.SolutionSettings.UserTypeForms, FormObj.RefId);
            }
            else if (WC == TokenConstants.MC && !string.IsNullOrEmpty(MobilePageRefId))
            {
                if (FormObj.SolutionObj?.SolutionSettings?.MobileAppSettings?.UserTypeForms == null)
                    return;
                CheckForMyProfileForms(FormObj, FormObj.SolutionObj.SolutionSettings.MobileAppSettings.UserTypeForms, MobilePageRefId);
            }
        }

        private void CheckForMyProfileForms(EbWebForm FormObj, List<EbProfileUserType> UserTypeForms, string RefId)
        {
            foreach (EbProfileUserType eput in UserTypeForms)
            {
                if (eput.RefId == RefId)
                {
                    //update eb_users_id

                    if (!FormObj.FormSchema.Tables[0].Columns.Exists(e => e.ColumnName == "eb_users_id"))
                    {
                        EbNumeric _numCtrl = new EbNumeric { Name = "eb_users_id", Label = "User Id" };
                        FormObj.Controls.Add(_numCtrl);
                        FormObj.FormSchema.Tables[0].Columns.Add(new ColumnSchema { ColumnName = "eb_users_id", EbDbType = (int)EbDbTypes.Int32, Control = _numCtrl });
                    }

                    if (FormObj.FormData.MultipleTables.ContainsKey(FormObj.FormSchema.MasterTable) && FormObj.FormData.MultipleTables[FormObj.FormSchema.MasterTable].Count > 0)
                    {
                        SingleColumn Column = FormObj.FormData.MultipleTables[FormObj.FormSchema.MasterTable][0].Columns.Find(e => e.Name == "eb_users_id");
                        if (Column == null)
                            FormObj.FormData.MultipleTables[FormObj.FormSchema.MasterTable][0].Columns.Add(new SingleColumn() { Name = "eb_users_id", Type = (int)EbDbTypes.Int32, Value = FormObj.UserObj.UserId });
                        else
                            Column.Value = FormObj.UserObj.UserId;
                    }
                    return;
                }
            }
        }

        public DeleteDataFromWebformResponse Any(DeleteDataFromWebformRequest request)
        {
            EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);
            CheckDataPusherCompatibility(FormObj);
            foreach (int _rowId in request.RowId)
            {
                FormObj.TableRowId = _rowId;
                int temp1 = FormObj.Delete(EbConnectionFactory.DataDB);
                if (SearchHelper.ExistsIndexControls(FormObj))
                    SearchHelper.Delete(EbConnectionFactory.DataDB, request.RefId, _rowId);
                Console.WriteLine($"Record deleted. RowId: {_rowId}  RowsAffected: {temp1}");
            }
            return new DeleteDataFromWebformResponse
            {
                RowAffected = request.RowId.Count()
            };
        }

        public CancelDataFromWebformResponse Any(CancelDataFromWebformRequest request)
        {
            EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);
            CheckDataPusherCompatibility(FormObj);
            FormObj.TableRowId = request.RowId;
            (int RowAffected, string modifiedAt) = FormObj.Cancel(EbConnectionFactory.DataDB, request.Cancel);
            Console.WriteLine($"Record cancelled. RowId: {request.RowId}  RowsAffected: {RowAffected}");

            return new CancelDataFromWebformResponse { RowAffected = RowAffected, ModifiedAt = modifiedAt };
        }

        public LockUnlockWebFormDataResponse Any(LockUnlockWebFormDataRequest request)
        {
            EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);
            FormObj.TableRowId = request.RowId;
            (int status, string modifiedAt) = FormObj.LockOrUnlock(this.EbConnectionFactory.DataDB, request.Lock);
            Console.WriteLine($"Record Lock/Unlock request. RowId: {request.RowId}  Status: {status}");

            return new LockUnlockWebFormDataResponse { Status = status, ModifiedAt = modifiedAt };
        }

        public GetPushedDataInfoResponse Any(GetPushedDataInfoRequest request)
        {
            try
            {
                Console.WriteLine($"GetPushedDataInfoRequest. RowId: {request.RowId} ");
                EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);
                EbSystemColumns ebs = FormObj.SolutionObj.SolutionSettings.SystemColumns;
                List<EbDataPusher> FormDp = FormObj.DataPushers.FindAll(e => e is EbFormDataPusher);

                foreach (EbBatchFormDataPusher batchDp in FormObj.DataPushers.FindAll(e => e is EbBatchFormDataPusher))
                {
                    EbWebForm _form = EbFormHelper.GetEbObject<EbWebForm>(batchDp.FormRefId, null, this.Redis, this);
                    _form.RefId = batchDp.FormRefId;
                    _form.AfterRedisGet_All(this);
                    batchDp.WebForm = _form;
                    FormDp.Add(batchDp);
                }
                string Qry = string.Empty;
                foreach (EbDataPusher dp in FormDp)
                {
                    string autoIdCol = string.Empty;
                    if (dp.WebForm.AutoId?.TableName == dp.WebForm.TableName)
                        autoIdCol = ", " + dp.WebForm.AutoId.Name;
                    Qry += $"SELECT id{autoIdCol} FROM {dp.WebForm.TableName} WHERE {FormObj.TableName}_id = {request.RowId} AND COALESCE({ebs[SystemColumns.eb_del]}, {ebs.GetBoolFalse(SystemColumns.eb_del)}) = {ebs.GetBoolFalse(SystemColumns.eb_del)}; ";
                }
                EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(Qry);
                Dictionary<string, string> resDict = new Dictionary<string, string>();
                List<string> Table_Id_s = new List<string>();
                for (int i = 0; i < FormDp.Count; i++)
                {
                    string autoIdCol = FormDp[i].WebForm.AutoId?.TableName == FormDp[i].WebForm.TableName ? FormDp[i].WebForm.AutoId.Name : null;
                    for (int j = 0; j < ds.Tables[i].Rows.Count; j++)
                    {
                        string id = Convert.ToString(ds.Tables[i].Rows[j][0]);
                        if (Table_Id_s.Contains(FormDp[i].WebForm.TableName + id))
                            continue;
                        Table_Id_s.Add(FormDp[i].WebForm.TableName + id);
                        string _p = JsonConvert.SerializeObject(new List<Param>() { { new Param { Name = "id", Type = "11", Value = id } } });
                        string link = $"/WebForm/Index?_r={FormDp[i].WebForm.RefId}&_p={_p.ToBase64()}&_m=1";
                        string autoIdVal = autoIdCol == null ? string.Empty : $" ({Convert.ToString(ds.Tables[i].Rows[j][1])})";
                        AddInDict_TryRec(resDict, FormDp[i].WebForm.DisplayName + autoIdVal, link, 1);
                    }
                }
                return new GetPushedDataInfoResponse { Result = JsonConvert.SerializeObject(resDict) };
            }
            catch (Exception e)
            {
                Console.WriteLine("Exception in GetPushedDataInfoRequest: " + e.Message);
                return new GetPushedDataInfoResponse { Result = $"Error Message: {e.Message}\n{e.StackTrace}" };
            }
        }

        private void AddInDict_TryRec(Dictionary<string, string> Dict, string Key, string Val, int I)
        {
            if (Dict.ContainsKey(Key))
            {
                if (Dict.ContainsKey($"{Key}({I})"))
                    AddInDict_TryRec(Dict, Key, Val, ++I);
                else
                    Dict.Add($"{Key}({I})", Val);
            }
            else
                Dict.Add(Key, Val);
        }

        //form data submission using PushJson and FormGlobals - SQL Job, Excel Import save
        public InsertOrUpdateFormDataResp Any(InsertOrUpdateFormDataRqst request)
        {
            try
            {
                Console.WriteLine("InsertOrUpdateFormDataRqst Service start");
                EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId, request.LocId);
                FormObj.TableRowId = request.RecordId;
                Console.WriteLine("InsertOrUpdateFormDataRqst PrepareWebFormData start : " + DateTime.Now);
                FormObj.PrepareWebFormData(this.EbConnectionFactory.DataDB, this, request.PushJson, request.FormGlobals);
                Console.WriteLine("InsertOrUpdateFormDataRqst Save start : " + DateTime.Now);
                string r = FormObj.Save(this.EbConnectionFactory.DataDB, this, request.TransactionConnection);
                Console.WriteLine("InsertOrUpdateFormDataRqst returning");
                return new InsertOrUpdateFormDataResp() { Status = (int)HttpStatusCode.OK, Message = "success", RecordId = FormObj.TableRowId };
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in InsertOrUpdateFormDataRqst\nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace : " + ex.StackTrace);
                return new InsertOrUpdateFormDataResp() { Status = ex.ExceptionCode, Message = ex.Message };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in InsertOrUpdateFormDataRqst\nMessage" + ex.Message + "\nStackTrace" + ex.StackTrace);
                return new InsertOrUpdateFormDataResp() { Status = (int)HttpStatusCode.InternalServerError, Message = ex.Message };
            }
        }

        public InsertBatchDataResponse Any(InsertBatchDataRequest request)
        {
            try
            {
                Console.WriteLine("InsertBatchDataRequest Service start");
                EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId, request.LocId);
                List<int> Ids = FormObj.ProcessBatchRequest(request.Data, this.EbConnectionFactory.DataDB, this, request.TransactionConnection);
                Console.WriteLine("InsertBatchDataRequest returning");
                return new InsertBatchDataResponse() { Status = (int)HttpStatusCode.OK, Message = "success", RecordIds = Ids };
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in InsertBatchDataRequest\nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace : " + ex.StackTrace);
                return new InsertBatchDataResponse() { Status = ex.ExceptionCode, Message = ex.Message };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in InsertBatchDataRequest\nMessage" + ex.Message + "\nStackTrace" + ex.StackTrace);
                return new InsertBatchDataResponse() { Status = (int)HttpStatusCode.InternalServerError, Message = ex.Message };
            }
        }

        public SaveFormDraftResponse Any(SaveFormDraftRequest request)
        {
            try
            {
                Console.WriteLine("SaveFormDraftRequest Service start");
                int Draft_id = 0;
                EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId, request.LocId);
                request.Title = string.IsNullOrEmpty(request.Title) ? FormObj.DisplayName : request.Title;
                if (request.DraftId <= 0)//new
                {
                    string Qry = $@"INSERT INTO eb_form_drafts (title, form_data_json, form_ref_id, is_submitted, eb_loc_id, eb_created_by, eb_created_at, eb_lastmodified_at, eb_del)
                                    VALUES (@title, @form_data_json, @form_ref_id, 'F', @eb_loc_id, @eb_created_by, {this.EbConnectionFactory.DataDB.EB_CURRENT_TIMESTAMP}, {this.EbConnectionFactory.DataDB.EB_CURRENT_TIMESTAMP}, 'F'); 
                                    SELECT eb_currval('eb_form_drafts_id_seq');";
                    DbParameter[] parameters = new DbParameter[]
                    {
                        this.EbConnectionFactory.DataDB.GetNewParameter("title", EbDbTypes.String, request.Title),
                        this.EbConnectionFactory.DataDB.GetNewParameter("form_data_json", EbDbTypes.String, request.Data),
                        this.EbConnectionFactory.DataDB.GetNewParameter("form_ref_id", EbDbTypes.String, FormObj.RefId),
                        this.EbConnectionFactory.DataDB.GetNewParameter("eb_loc_id", EbDbTypes.Int32, request.LocId),
                        this.EbConnectionFactory.DataDB.GetNewParameter("eb_created_by", EbDbTypes.Int32, request.UserId)
                    };
                    EbDataSet ds = this.EbConnectionFactory.DataDB.DoQueries(Qry, parameters);
                    Draft_id = Convert.ToInt32(ds.Tables[0].Rows[0][0]);
                    if (Draft_id == 0)
                        throw new FormException("Something went wrong in our end.", (int)HttpStatusCode.InternalServerError, $"SELECT eb_currval('eb_form_drafts_id_seq') returned 0", "SaveFormDraftRequest -> New");
                }
                else
                {
                    string Qry = $@"UPDATE eb_form_drafts SET title = @title, form_data_json = @form_data_json, eb_loc_id = @eb_loc_id, eb_lastmodified_at = {this.EbConnectionFactory.DataDB.EB_CURRENT_TIMESTAMP}
                                    WHERE id = @id AND form_ref_id = @form_ref_id AND eb_created_by = @eb_created_by AND is_submitted = 'F' AND eb_del = 'F';";
                    DbParameter[] parameters = new DbParameter[]
                    {
                        this.EbConnectionFactory.DataDB.GetNewParameter("title", EbDbTypes.String, request.Title),
                        this.EbConnectionFactory.DataDB.GetNewParameter("form_data_json", EbDbTypes.String, request.Data),
                        this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.DraftId),
                        this.EbConnectionFactory.DataDB.GetNewParameter("form_ref_id", EbDbTypes.String, FormObj.RefId),
                        this.EbConnectionFactory.DataDB.GetNewParameter("eb_loc_id", EbDbTypes.Int32, request.LocId),
                        this.EbConnectionFactory.DataDB.GetNewParameter("eb_created_by", EbDbTypes.Int32, request.UserId)
                    };
                    int status = this.EbConnectionFactory.DataDB.DoNonQuery(Qry, parameters);
                    if (status == 0)
                        throw new FormException("Not Found.", (int)HttpStatusCode.NotFound, $"No row affected", "SaveFormDraftRequest -> Edit");
                    Draft_id = request.DraftId;
                }
                Console.WriteLine("SaveFormDraftRequest returning");
                return new SaveFormDraftResponse() { Status = (int)HttpStatusCode.OK, Message = "success", DraftId = Draft_id };
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in SaveFormDraftRequest\nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace : " + ex.StackTrace);
                return new SaveFormDraftResponse() { Status = ex.ExceptionCode, Message = ex.Message };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in SaveFormDraftRequest\nMessage" + ex.Message + "\nStackTrace" + ex.StackTrace);
                return new SaveFormDraftResponse() { Status = (int)HttpStatusCode.InternalServerError, Message = ex.Message };
            }
        }

        public DiscardFormDraftResponse Any(DiscardFormDraftRequest request)
        {
            try
            {
                Console.WriteLine("DiscardFormDraftRequest Service start");
                EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);////////

                string Qry = $@"UPDATE eb_form_drafts SET eb_del = 'T', eb_lastmodified_at = {this.EbConnectionFactory.DataDB.EB_CURRENT_TIMESTAMP}
                                    WHERE id = @id AND form_ref_id = @form_ref_id AND eb_created_by = @eb_created_by AND is_submitted = 'F' AND eb_del = 'F';";
                DbParameter[] parameters = new DbParameter[]
                {
                    this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.DraftId),
                    this.EbConnectionFactory.DataDB.GetNewParameter("form_ref_id", EbDbTypes.String, FormObj.RefId),
                    this.EbConnectionFactory.DataDB.GetNewParameter("eb_created_by", EbDbTypes.Int32, request.UserId)
                };
                int status = this.EbConnectionFactory.DataDB.DoNonQuery(Qry, parameters);
                if (status == 0)
                    throw new FormException("Not Found.", (int)HttpStatusCode.NotFound, $"No row affected", "SaveFormDraftRequest -> Edit");
                Console.WriteLine("SaveFormDraftRequest returning");
                return new DiscardFormDraftResponse() { Status = (int)HttpStatusCode.OK, Message = "success" };
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in DiscardFormDraft\nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace : " + ex.StackTrace);
                return new DiscardFormDraftResponse() { Status = ex.ExceptionCode, Message = ex.Message };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in DiscardFormDraft\nMessage" + ex.Message + "\nStackTrace" + ex.StackTrace);
                return new DiscardFormDraftResponse() { Status = (int)HttpStatusCode.InternalServerError, Message = ex.Message };
            }
        }

        public GetFormDraftResponse Any(GetFormDraftRequest request)
        {
            try
            {
                Console.WriteLine("GetFormDraftRequest Service start");
                EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);////////
                string Json = string.Empty;
                string Qry = $@"SELECT id, form_data_json, eb_loc_id, eb_created_by, eb_created_at, eb_lastmodified_at FROM eb_form_drafts
                                    WHERE id = @id AND form_ref_id = @form_ref_id AND eb_created_by = @eb_created_by AND is_submitted = 'F' AND eb_del = 'F';";
                DbParameter[] parameters = new DbParameter[]
                {
                    this.EbConnectionFactory.DataDB.GetNewParameter("id", EbDbTypes.Int32, request.DraftId),
                    this.EbConnectionFactory.DataDB.GetNewParameter("form_ref_id", EbDbTypes.String, FormObj.RefId),
                    this.EbConnectionFactory.DataDB.GetNewParameter("eb_created_by", EbDbTypes.Int32, request.UserId)
                };
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(Qry, parameters);
                if (dt.Rows.Count == 0)
                    throw new FormException("Not Found.", (int)HttpStatusCode.NotFound, $"Record not found", "SaveFormDraftRequest -> Edit");
                else
                    Json = Convert.ToString(dt.Rows[0][1]);
                Console.WriteLine("GetFormDraftRequest returning");

                WebformDataWrapper dataWrapper = new WebformDataWrapper { Status = (int)HttpStatusCode.OK, Message = "success", FormData = new WebformData() };
                string p = FormObj.UserObj.Preference.GetShortDatePattern() + " " + FormObj.UserObj.Preference.GetLongTimePattern();
                int locid = Convert.ToInt32(dt.Rows[0][2]), uid = Convert.ToInt32(dt.Rows[0][3]);
                DateTime dt1 = Convert.ToDateTime(dt.Rows[0][4]).ConvertFromUtc(FormObj.UserObj.Preference.TimeZone);
                DateTime dt2 = Convert.ToDateTime(dt.Rows[0][5]).ConvertFromUtc(FormObj.UserObj.Preference.TimeZone);
                string temp = FormObj.SolutionObj.Users.ContainsKey(uid) ? FormObj.SolutionObj.Users[uid] : string.Empty;
                WebformDataInfo Info = new WebformDataInfo()
                {
                    CreAt = dt1.ToString(p, CultureInfo.InvariantCulture),
                    CreBy = temp,
                    ModAt = dt2.ToString(p, CultureInfo.InvariantCulture),
                    ModBy = temp,
                    CreFrom = FormObj.SolutionObj.Locations.ContainsKey(locid) ? FormObj.SolutionObj.Locations[locid].ShortName : string.Empty
                };
                temp = JsonConvert.SerializeObject(Info);
                return new GetFormDraftResponse() { DataWrapper = JsonConvert.SerializeObject(dataWrapper), FormDatajson = Json, DraftInfo = temp };
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in GetFormDraftRequest\nMessage : " + ex.Message + "\nMessageInternal : " + ex.MessageInternal + "\nStackTraceInternal : " + ex.StackTraceInternal + "\nStackTrace : " + ex.StackTrace);

                return new GetFormDraftResponse() { DataWrapper = JsonConvert.SerializeObject(new WebformDataWrapper { Status = ex.ExceptionCode, Message = ex.Message }) };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in GetFormDraftRequest\nMessage" + ex.Message + "\nStackTrace" + ex.StackTrace);
                return new GetFormDraftResponse() { DataWrapper = JsonConvert.SerializeObject(new WebformDataWrapper { Status = (int)HttpStatusCode.InternalServerError, Message = ex.Message }) };
            }
        }

        public CheckEmailAndPhoneResponse Any(CheckEmailAndPhoneRequest request)
        {
            Dictionary<string, ChkEmailPhoneReqData> _data = JsonConvert.DeserializeObject<Dictionary<string, ChkEmailPhoneReqData>>(request.Data);
            string _selQry = "SELECT id, fullname, email, phnoprimary FROM eb_users WHERE LOWER($) LIKE LOWER(@#) AND COALESCE(eb_del, 'F') = 'F' AND ((statusid >= 0 AND statusid <= 2) OR statusid = 4); ";
            string _selQryDummy = "SELECT 1 WHERE 1 = 0; ";
            IDatabase DataDB = this.EbConnectionFactory.DataDB;
            string Qry = string.Empty;
            List<DbParameter> parameters = new List<DbParameter>();
            foreach (string ctrlName in _data.Keys)
            {
                if (!string.IsNullOrEmpty(_data[ctrlName].email))
                {
                    Qry += _selQry.Replace("#", ctrlName + "_em").Replace("$", "email");
                    parameters.Add(DataDB.GetNewParameter(ctrlName + "_em", EbDbTypes.String, _data[ctrlName].email));
                }
                else
                    Qry += _selQryDummy;

                if (!string.IsNullOrEmpty(_data[ctrlName].phprimary))
                {
                    Qry += _selQry.Replace("#", ctrlName + "_ph").Replace("$", "phnoprimary");
                    parameters.Add(DataDB.GetNewParameter(ctrlName + "_ph", EbDbTypes.String, _data[ctrlName].phprimary));
                }
                else
                    Qry += _selQryDummy;

                if (_data[ctrlName].id > 1)
                {
                    Qry += $"SELECT id, fullname, email, phnoprimary FROM eb_users WHERE id = @{ctrlName}_id; ";
                    parameters.Add(DataDB.GetNewParameter(ctrlName + "_id", EbDbTypes.Int32, _data[ctrlName].id));
                }
            }
            Dictionary<string, ChkEmailPhoneRespData> Resp = new Dictionary<string, ChkEmailPhoneRespData>();
            if (Qry != string.Empty)
            {
                EbDataSet ds = DataDB.DoQueries(Qry, parameters.ToArray());
                int index = 0;
                RowColletion dr;
                foreach (string ctrlName in _data.Keys)
                {
                    Resp.Add(ctrlName, new ChkEmailPhoneRespData());
                    dr = ds.Tables[index++].Rows;
                    if (dr.Count > 0)
                    {
                        Resp[ctrlName].emailData = new ChkEmailPhoneReqData()
                        {
                            id = Convert.ToInt32(dr[0][0]),
                            fullname = Convert.ToString(dr[0][1]),
                            email = Convert.ToString(dr[0][2]),
                            phprimary = Convert.ToString(dr[0][3]),
                        };
                    }
                    dr = ds.Tables[index++].Rows;
                    if (dr.Count > 0)
                    {
                        Resp[ctrlName].phoneData = new ChkEmailPhoneReqData()
                        {
                            id = Convert.ToInt32(dr[0][0]),
                            fullname = Convert.ToString(dr[0][1]),
                            email = Convert.ToString(dr[0][2]),
                            phprimary = Convert.ToString(dr[0][3]),
                        };
                    }
                    if (_data[ctrlName].id > 1 && dr.Count > 0)
                    {
                        dr = ds.Tables[index++].Rows;
                        Resp[ctrlName].curData = new ChkEmailPhoneReqData()
                        {
                            id = Convert.ToInt32(dr[0][0]),
                            fullname = Convert.ToString(dr[0][1]),
                            email = Convert.ToString(dr[0][2]),
                            phprimary = Convert.ToString(dr[0][3]),
                        };
                    }
                }
            }
            return new CheckEmailAndPhoneResponse() { Data = JsonConvert.SerializeObject(Resp) };
        }

        private class ChkEmailPhoneReqData
        {
            public int id { get; set; }
            public string fullname { get; set; }
            public string email { get; set; }
            public string phprimary { get; set; }
        }
        private class ChkEmailPhoneRespData
        {
            public string status { get; set; }
            public ChkEmailPhoneReqData emailData { get; set; }
            public ChkEmailPhoneReqData phoneData { get; set; }
            public ChkEmailPhoneReqData curData { get; set; }
        }

        private enum EmailPhoneStatus
        {
            CancelOperation = 0,
            Insert = 1,
            Update = 2,
            Created = 4,
            Linked = 8,
            NothingUnique = 16,
            EmailNotUnique = 32,
            PhoneNotUnique = 64,
            EmailUnique = 128,
            PhoneUnique = 256,
            EmailPhoneUnique = 512,
        }

        public GetProvUserListResponse Any(GetProvUserListRequest request)
        {
            string Qry = @"SELECT id, fullname, email, phnoprimary FROM eb_users WHERE COALESCE(eb_del, 'F') = 'F' AND id > 1 AND statusid >= 0 AND statusid <= 2 ORDER BY fullname, email, phnoprimary;";
            EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(Qry);
            List<Eb_Users> _usersListAll = new List<Eb_Users>();
            foreach (EbDataRow dr in dt.Rows)
            {
                _usersListAll.Add(new Eb_Users()
                {
                    Id = Convert.ToInt32(dr[0]),
                    Name = Convert.ToString(dr[1]),
                    Email = Convert.ToString(dr[2]),
                    Phone = Convert.ToString(dr[3])
                });
            }
            return new GetProvUserListResponse() { Data = JsonConvert.SerializeObject(_usersListAll) };
        }

        public GetGlobalSrchRsltsResp Any(GetGlobalSrchRsltsReq request)
        {
            Eb_Solution SlnObj = this.GetSolutionObject(request.SolnId);
            User UsrObj = GetUserObject(request.UserAuthId);
            string Json = SearchHelper.GetSearchResults(this.EbConnectionFactory.DataDB, SlnObj, UsrObj, request.SrchText);
            return new GetGlobalSrchRsltsResp() { Data = Json };
        }

        public UpdateIndexesRespone Any(UpdateIndexesRequest request)
        {
            string msg;
            try
            {
                if (request.RefId == "leadmanagement")
                {
                    msg = SearchHelper.UpdateIndexes_LM(this.EbConnectionFactory.DataDB, request.Limit, request.Offset);
                }
                else
                {
                    EbWebForm FormObj = this.GetWebFormObject(request.RefId, request.UserAuthId, request.SolnId);
                    msg = SearchHelper.UpdateIndexes(this.EbConnectionFactory.DataDB, FormObj, request.Limit);
                }
            }
            catch (Exception e)
            {
                msg = "ERROR : " + e.Message;
            }
            return new UpdateIndexesRespone() { Message = msg };
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
                throw new FormException(ex.Message);
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

        ///// for retriving row id and question name for questionnaire configuration control
        public GetQuestionsBankResponse Get(GetQuestionsBankRequest request)
        {
            GetQuestionsBankResponse resp = new GetQuestionsBankResponse() { Questionlst = new Dictionary<int, string>() };
            try
            {
                string Qry = @"SELECT id,question FROM eb_question_bank WHERE COALESCE(eb_del, 'F')='F';";
                EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(Qry);
                List<Eb_Users> _usersListAll = new List<Eb_Users>();
                if (dt.Rows.Count > 0)
                {
                    foreach (EbDataRow dr in dt.Rows)
                    {
                        resp.Questionlst.Add(Convert.ToInt32(dr[0]), Convert.ToString(dr[1]));
                    }
                }


            }
            catch (Exception e)
            {
                Console.WriteLine("EXCEPTION: while getting questions from questionbank \nMessage: " + e.Message + "\nstacktrace:" + e.StackTrace);
            }
            return resp;

        }

        public UpdateAllFormTablesResponse Post(UpdateAllFormTablesRequest request)
        {
            string msg = $"Start* UpdateAllFormTables {DateTime.Now}\n\n";
            try
            {
                User u = GetUserObject(request.UserAuthId);
                if (u.Roles.Contains(SystemRoles.SolutionOwner.ToString()))
                {
                    string Qry = @"SELECT refid, display_name, obj_json, eovid FROM (
				                        SELECT 
					                        EO.id, EOV.id AS eovid, EOV.refid, EO.display_name, EOV.obj_json
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
	                                    WHERE COALESCE(EO2A.eb_del, 'F') = 'F' LIMIT 1000;";

                    EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(Qry);
                    msg += $"Form Objects Count : {dt.Rows.Count} \n";
                    Eb_Solution SolutionObj = this.GetSolutionObject(request.SolnId);
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
                                if (string.IsNullOrWhiteSpace(request.InMsg))
                                {
                                    F.AutoDeployTV = false;
                                    try
                                    {
                                        this.Any(new CreateWebFormTableRequest { WebObj = F, SolnId = request.SolnId, SoluObj = SolutionObj });
                                        msg += $"\n\nSuccess   RefId : {dr[0].ToString()}, Name : {dr[1].ToString()} ";
                                    }
                                    catch (Exception e)
                                    {
                                        msg += $"\n\nWarning   RefId : {dr[0].ToString()}, Name : {dr[1].ToString()}, Message : {e.Message} ";
                                    }
                                }
                                else if (request.InMsg == "save")
                                {
                                    try
                                    {
                                        F.BeforeSave(this);
                                        string json = EbSerializers.Json_Serialize(F);
                                        string _qry = $"UPDATE eb_objects_ver SET obj_json = @obj_jsonv WHERE id={dr[3]} AND refid='{dr[0]}';";
                                        int i = this.EbConnectionFactory.DataDB.DoNonQuery(_qry, new DbParameter[]
                                        {
                                            this.EbConnectionFactory.DataDB.GetNewParameter("obj_jsonv", EbDbTypes.Json, json)
                                        });
                                        if (i == 1)
                                        {
                                            Redis.Set(dr[0].ToString(), F);
                                            msg += $"\n\nSuccess[OBJ_SAVE]   RefId : {dr[0].ToString()}, Name : {dr[1].ToString()} ";
                                        }
                                        else
                                            msg += $"\n\nWarning[OBJ_SAVE]   RefId : {dr[0].ToString()}, Name : {dr[1].ToString()}, Message : DoNonQuery returned {i}";
                                    }
                                    catch (Exception e)
                                    {
                                        msg += $"\n\nWarning[OBJ_SAVE]   RefId : {dr[0].ToString()}, Name : {dr[1].ToString()}, Message : {e.Message} ";
                                    }
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
            EbProfileUserType userType = new EbProfileUserType();
            GetMyProfileEntryResponse response = new GetMyProfileEntryResponse();
            try
            {
                string q1 = $"SELECT eb_user_types_id FROM eb_users WHERE id = {request.UserId} ;";
                EbDataTable dt1 = this.EbConnectionFactory.DataDB.DoQuery(q1);
                if (dt1.Rows.Count > 0)
                {
                    int type_id = Convert.ToInt32(dt1.Rows[0][0]);
                    Eb_Solution solutionObj = GetSolutionObject(request.SolnId);
                    if (solutionObj?.SolutionSettings != null)
                    {
                        if (request.WhichConsole == RoutingConstants.MC)
                            userType = solutionObj.SolutionSettings.MobileAppSettings?.UserTypeForms?.Single(a => a.Id == type_id);
                        else
                            userType = solutionObj.SolutionSettings.UserTypeForms?.Single(a => a.Id == type_id);

                        if (!string.IsNullOrEmpty(userType?.RefId))
                        {
                            response.Refid = userType.RefId;

                            EbObjectService myService = base.ResolveService<EbObjectService>();
                            EbObjectParticularVersionResponse resp = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = userType.RefId });
                            if (resp != null)
                            {
                                EbObject ebObj = EbSerializers.Json_Deserialize<EbObject>(resp.Data[0].Json);
                                if (ebObj != null)
                                {
                                    string tablename = string.Empty;
                                    if (ebObj is EbMobilePage page && page.Container is EbMobileForm mobileForm)
                                        tablename = mobileForm.TableName;
                                    else if (ebObj is EbWebForm webForm)
                                        tablename = webForm.TableName;
                                    try
                                    {
                                        string q2 = string.Format("SELECT id from {0} where eb_users_id = {1};", tablename, request.UserId);
                                        EbDataTable dt2 = this.EbConnectionFactory.DataDB.DoQuery(q2);

                                        if (dt2.Rows.Count > 0)
                                        {
                                            response.RowId = Convert.ToInt32(dt2.Rows[0][0]);
                                            response.ProfileExist = true;
                                        }
                                    }
                                    catch (Exception ex)
                                    {
                                        Console.WriteLine($"failed to fetch profile id from table '{tablename}' for user [{request.UserId}], " + ex.Message);
                                    }
                                }
                                else
                                    response.ErrorMessage += $"Form '{userType.RefId}' is unavailable";
                            }
                        }
                        else
                            response.ErrorMessage += "User type form is empty";
                    }
                    else
                        response.ErrorMessage += "Solution is unavailable or Solution settings is empty";
                }
                else
                    response.ErrorMessage += "User type is not set for this user";
            }
            catch (Exception e)
            {
                Console.WriteLine(e.Message + e.StackTrace);
            }
            return response;
        }
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
