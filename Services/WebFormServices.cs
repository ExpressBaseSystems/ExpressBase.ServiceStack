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
using System.Dynamic;
using System.Globalization;
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
                        EbWebForm _form = GetWebFormObject(pusher.FormRefId);
                        TableSchema _table = _form.FormSchema.Tables.Find(e => e.TableName.Equals(_form.FormSchema.MasterTable));
                        //_table.Columns.Add(new ColumnSchema { ColumnName = "eb_push_id", EbDbType = (int)EbDbTypes.String, Control = new EbTextBox { Name = "eb_push_id", Label = "Push Id" } });// multi push id
                        //_table.Columns.Add(new ColumnSchema { ColumnName = "eb_src_id", EbDbType = (int)EbDbTypes.Decimal, Control = new EbNumeric { Name = "eb_src_id", Label = "Source Id" } });// source master table id
                        if (_table != null)
                            Form.FormSchema.Tables.Add(_table);
                    }
                }
                CreateWebFormTables(Form.FormSchema, request);
            }
            return new CreateWebFormTableResponse { };
        }

        private void CreateWebFormTables(WebFormSchema _schema, CreateWebFormTableRequest request)
        {
            IVendorDbTypes vDbTypes = this.EbConnectionFactory.DataDB.VendorDbTypes;
            string Msg = string.Empty;
            foreach (TableSchema _table in _schema.Tables)
            {
                List<TableColumnMeta> _listNamesAndTypes = new List<TableColumnMeta>();
                if (_table.Columns.Count > 0)
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
                        _listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_row_num", Type = vDbTypes.Decimal });// data grid row number

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
                        CreateOrUpdateDsAndDv(request, _listNamesAndTypes);
                }
            }
            if (!Msg.IsEmpty())
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
                                Msg += string.Format("Already exists '{0}' Column for {1}.{2}({3}); ", dr.Type.ToString(), tableName, entry.Name, entry.Type.EbDbType);
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
            IEnumerable<TableColumnMeta> _list = listNamesAndTypes.Where(x => x.Name != "eb_del" && x.Name != "eb_ver_id" && !(x.Name.Contains("_ebbkup")) && x.Name != "eb_push_id" && x.Name != "eb_src_id" && x.Name != "eb_lock" && x.Name != "eb_signin_log_id" && !(x.Control is EbFileUploader));
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
                UpdateDataVisualization(request, _list.ToList(), dv, AutogenId);
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
            RowGroupParent _rowgroup = new RowGroupParent();
            _rowgroup.DisplayName = "By Location";
            _rowgroup.Name = "groupbylocation";
            _rowgroup.RowGrouping.Add(columns.Get("eb_loc_id"));

            dvobj.RowGroupCollection.Add(_rowgroup);
            _rowgroup = new RowGroupParent();
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
                if (column.Name != "eb_del" && column.Name != "eb_ver_id" && !(column.Name.Contains("_ebbkup")) && !(column.Control is EbFileUploader))
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
                        if (!(column.Control as EbPowerSelect).MultiSelect)
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
                    if (_RenderType == EbDbTypes.String)
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

            Columns.Add(new DVStringColumn
            {
                Data = (index + 1),//index+1 for serial column in datavis service
                Name = "eb_action",
                sTitle = "Action",
                Type = EbDbTypes.String,
                bVisible = true,
                sWidth = "100px",
                ClassName = "tdheight",
                RenderAs = StringRenderType.Link,
                LinkRefId = request.WebObj.RefId,
                LinkType = LinkTypeEnum.Popout,
                FormMode = WebFormDVModes.View_Mode,
                FormId = _formid,
                Align = Align.Center
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
                if (_col == null || _col.Name == "eb_void" || column.Name == "eb_created_by" || column.Name == "eb_lastmodified_by" || column.Name == "eb_loc_id" || column.Control is EbPowerSelect)
                {
                    index++;
                    ControlClass _control = null;
                    bool _autoresolve = false;
                    Align _align = Align.Auto;
                    int charlength = 0;
                    EbDbTypes _RenderType = column.Type.EbDbType;
                    if (column.Control is EbPowerSelect)
                    {
                        if (!(column.Control as EbPowerSelect).MultiSelect)
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
                    }
                    else if (column.Control is EbTextBox)
                    {
                        if ((column.Control as EbTextBox).TextMode == TextMode.MultiLine)
                        {
                            charlength = 20;
                        }
                    }
                    if (column.Name == "eb_void")
                    {
                        column.Type = vDbTypes.String;//T or F
                        _RenderType = EbDbTypes.Boolean;
                    }
                    else if (column.Name == "eb_created_by" || column.Name == "eb_lastmodified_by" || column.Name == "eb_loc_id")
                    {
                        _RenderType = EbDbTypes.String;
                    }
                    if (_RenderType == EbDbTypes.String)
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
                    {
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
                    }
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
                    _col.RenderType = column.Type.EbDbType;
                    _col.Data = ++index;
                    Columns.Add(_col);
                }

            }
            Columns.Add(dv.Columns.Get("id"));
            DVBaseColumn Col = dv.Columns.Get("eb_action");
            Col.Data = ++index;
            Columns.Add(Col);
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
                EbWebForm form = GetWebFormObject(request.RefId);
                form.TableRowId = request.RowId;
                form.RefId = request.RefId;
                form.UserObj = request.UserObj;
                form.SolutionObj = GetSolutionObject(request.SolnId);
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
                if (!(form.HasPermission(OperationConstants.VIEW, form.LocationId) || form.HasPermission(OperationConstants.NEW, form.LocationId) || form.HasPermission(OperationConstants.EDIT, form.LocationId)))
                {
                    throw new FormException("Error in loading data. Access Denied.", (int)HttpStatusCodes.UNAUTHORIZED, "Access Denied for rowid " + form.TableRowId + " , current location " + form.LocationId, string.Empty);
                }
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper()
                {
                    FormData = form.FormData,
                    Status = (int)HttpStatusCodes.OK,
                    Message = "Success"
                });
                Console.WriteLine("Returning from GetRowData Service");
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in GetRowData Service \nMessage : " + ex.Message + "\n" + ex.StackTrace);
                _dataset.FormDataWrap = JsonConvert.SerializeObject(new WebformDataWrapper() { Message = ex.Message, Status = ex.ExceptionCode, MessageInt = ex.MessageInternal, StackTraceInt = ex.StackTraceInternal });
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in GetRowData Service \nMessage : " + ex.Message + "\n" + ex.StackTrace);
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
                EbWebForm form = GetWebFormObject(request.RefId);
                form.RefId = request.RefId;
                form.RefreshFormData(EbConnectionFactory.DataDB, this, request.Params);
                _dataset.FormData = new WebformDataWrapper { FormData = form.FormData, Status = (int)HttpStatusCodes.OK, Message = "Success" };
            }
            catch (FormException ex)
            {
                _dataset.FormData = new WebformDataWrapper { Message = ex.Message, Status = ex.ExceptionCode, MessageInt = ex.MessageInternal, StackTraceInt = ex.StackTraceInternal };
            }
            catch (Exception e)
            {
                _dataset.FormData = new WebformDataWrapper { Message = "Something went wrong.", Status = (int)HttpStatusCodes.INTERNAL_SERVER_ERROR, MessageInt = e.Message, StackTraceInt = e.StackTrace };
            }
            Console.WriteLine("End GetPrefillData");
            return _dataset;
        }

        //public GetImportDataResponse Any(GetImportDataRequest request)
        //{
        //    try
        //    {
        //        DataSourceService myService = base.ResolveService<DataSourceService>();
        //        DataSourceDataSetResponse response = (DataSourceDataSetResponse)myService.Any(new DataSourceDataSetRequest() { RefId = request.RefId, Params = request.Params });
        //        SingleTable Table = new SingleTable();
        //        EbWebForm WebForm = new EbWebForm();
        //        WebForm.GetFormattedData(response.DataSet.Tables[0], Table);
        //        WebformData formData = new WebformData { MultipleTables = new Dictionary<string, SingleTable>() { { "Table1", Table } } };
        //        return new GetImportDataResponse() { FormData = formData };
        //    }
        //    catch(Exception ex)
        //    {
        //        Console.WriteLine("Exception in GetImportDataRequest Service" + ex.Message);
        //        Console.WriteLine(ex.StackTrace);
        //        throw ex;
        //    }
        //}

        public GetImportDataResponse Any(GetImportDataRequest request)
        {
            GetImportDataResponse resp = new GetImportDataResponse();
            try
            {
                Console.WriteLine("Start ImportFormData");
                EbWebForm form = GetWebFormObject(request.RefId);
                form.RefId = request.RefId;
                form.UserObj = this.Redis.Get<User>(request.UserAuthId);
                form.SolutionObj = GetSolutionObject(request.SolnId);
                form.ImportData(EbConnectionFactory.DataDB, this, request.Params, request.Trigger);
                Console.WriteLine("End ImportFormData");
                resp.FormDataWrap = new WebformDataWrapper { FormData = form.FormData, Status = (int)HttpStatusCodes.OK, Message = "Success" };
            }
            catch (FormException ex)
            {
                Console.WriteLine("FormException in GetImportDataRequest Service" + ex.Message);
                resp.FormDataWrap = new WebformDataWrapper { Status = ex.ExceptionCode, Message = ex.Message, MessageInt = ex.MessageInternal, StackTraceInt = ex.StackTraceInternal };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in GetImportDataRequest Service" + ex.Message + "\n" + ex.StackTrace);
                resp.FormDataWrap = new WebformDataWrapper { Status = (int)HttpStatusCodes.INTERNAL_SERVER_ERROR, Message = "Exception in GetImportDataRequest", MessageInt = ex.Message, StackTraceInt = ex.StackTrace };
            }
            return resp;
        }

        public ExecuteSqlValueExprResponse Any(ExecuteSqlValueExprRequest request)
        {
            Console.WriteLine("Start ExecuteSqlValueExpr");
            EbWebForm form = GetWebFormObject(request.RefId);
            string val = form.ExecuteSqlValueExpression(EbConnectionFactory.DataDB, this, request.Params, request.Trigger);
            Console.WriteLine("End ExecuteSqlValueExpr");
            return new ExecuteSqlValueExprResponse() { Data = val };
        }

        public GetDataPusherJsonResponse Any(GetDataPusherJsonRequest request)
        {
            Console.WriteLine("Start GetDataPusherJson");
            EbWebForm form = GetWebFormObject(request.RefId);
            string val = form.GetDataPusherJson();
            Console.WriteLine("End GetDataPusherJson");
            return new GetDataPusherJsonResponse() { Json = val };
        }


        private EbWebForm GetWebFormObject(string RefId)
        {
            EbWebForm _form = this.Redis.Get<EbWebForm>(RefId);
            if (_form == null)
            {
                var myService = base.ResolveService<EbObjectService>();
                EbObjectParticularVersionResponse formObj = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = RefId });
                _form = EbSerializers.Json_Deserialize(formObj.Data[0].Json);
                this.Redis.Set<EbWebForm>(RefId, _form);
            }
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
                EbWebForm FormObj = GetWebFormObject(request.RefId);
                FormObj.RefId = request.RefId;
                FormObj.TableRowId = request.RowId;
                FormObj.FormData = request.FormData;
                FormObj.UserObj = request.UserObj;
                FormObj.LocationId = request.CurrentLoc;
                FormObj.SolutionObj = GetSolutionObject(request.SolnId);

                string Operation = OperationConstants.NEW;
                if (request.RowId > 0)
                    Operation = OperationConstants.EDIT;
                if (!FormObj.HasPermission(Operation, request.CurrentLoc))
                    return new InsertDataFromWebformResponse { Status = (int)HttpStatusCodes.FORBIDDEN, Message = "Access denied to save this data entry!", MessageInt = "Access denied" };

                Console.WriteLine("Insert/Update WebFormData : MergeFormData start - " + DateTime.Now);
                FormObj.MergeFormData();
                Console.WriteLine("Insert/Update WebFormData : Save start - " + DateTime.Now);
                string r = FormObj.Save(EbConnectionFactory.DataDB, this);
                if (this.EbConnectionFactory.EmailConnection != null && this.EbConnectionFactory.EmailConnection.Primary != null)
                {
                    Console.WriteLine("Insert/Update WebFormData : SendMailIfUserCreated start - " + DateTime.Now);
                    FormObj.SendMailIfUserCreated(MessageProducer3);
                }
                Console.WriteLine("Insert/Update WebFormData : Execution Time = " + (DateTime.Now - startdt).TotalMilliseconds);
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
                Console.WriteLine("Exception in Insert/Update WebFormData" + ex.Message + "\n" + ex.StackTrace);
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
                Console.WriteLine("Exception in Insert/Update WebFormData" + ex.Message + "\n" + ex.StackTrace);
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
            EbWebForm FormObj = GetWebFormObject(request.RefId);
            FormObj.TableRowId = request.RowId;
            FormObj.UserObj = request.UserObj;
            return new DeleteDataFromWebformResponse
            {
                RowAffected = FormObj.Delete(EbConnectionFactory.DataDB)
            };
        }

        public CancelDataFromWebformResponse Any(CancelDataFromWebformRequest request)
        {
            EbWebForm FormObj = GetWebFormObject(request.RefId);
            FormObj.TableRowId = request.RowId;
            FormObj.UserObj = request.UserObj;
            return new CancelDataFromWebformResponse
            {
                RowAffected = FormObj.Cancel(EbConnectionFactory.DataDB)
            };
        }

        public InsertOrUpdateFormDataResp Any(InsertOrUpdateFormDataRqst request)
        {
            try
            {
                EbWebForm FormObj = GetWebFormObject(request.RefId);
                FormObj.RefId = request.RefId;
                FormObj.TableRowId = request.RecordId;
                FormObj.UserObj = request.UserObj;
                FormObj.LocationId = request.LocId;
                FormObj.SolutionObj = GetSolutionObject(request.SolnId);
                Console.WriteLine("InsertOrUpdateFormDataRqst PrepareWebFormData start : " + DateTime.Now);
                FormObj.PrepareWebFormData(this.EbConnectionFactory.DataDB, this, request.PushJson, request.FormGlobals);
                Console.WriteLine("InsertOrUpdateFormDataRqst Save start : " + DateTime.Now);
                string r = FormObj.Save(this.EbConnectionFactory.DataDB);
                Console.WriteLine("InsertOrUpdateFormDataRqst returning");
                return new InsertOrUpdateFormDataResp() { Status = (int)HttpStatusCodes.OK, Message = "success", RecordId = FormObj.TableRowId };
            }
            catch (FormException ex)
            {
                Console.WriteLine("Exception in InsertOrUpdateFormDataRqst" + ex.Message + "\n" + ex.MessageInternal + "\n" + ex.StackTrace);
                return new InsertOrUpdateFormDataResp() { Status = ex.ExceptionCode, Message = ex.Message };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in InsertOrUpdateFormDataRqst" + ex.Message + "\n" + ex.StackTrace);
                return new InsertOrUpdateFormDataResp() { Status = (int)HttpStatusCodes.INTERNAL_SERVER_ERROR, Message = ex.Message };
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
                EbWebForm FormObj = GetWebFormObject(request.FormId);
                FormObj.RefId = request.FormId;
                FormObj.TableRowId = request.RowId;
                FormObj.UserObj = request.UserObj;
                FormObj.SolutionObj = GetSolutionObject(request.SolnId);

                string temp = FormObj.GetAuditTrail(EbConnectionFactory.DataDB, this);

                return new GetAuditTrailResponse() { Json = temp };
            }
            catch (Exception ex)
            {
                Console.WriteLine("Exception in GetAuditTrail Service" + ex.Message);
                Console.WriteLine(ex.StackTrace);
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
            EbWebForm form = this.GetWebFormObject(request.RefId);

            IEnumerable<EbControl> ctrls = form.Controls.FlattenEbControls();

            return new GetCtrlsFlatResponse { Controls = ctrls.ToList<EbControl>() };
        }

        public CheckEmailConAvailableResponse Post(CheckEmailConAvailableRequest request)
        {
            return new CheckEmailConAvailableResponse { ConnectionAvailable = this.EbConnectionFactory.EmailConnection.Primary != null };
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
    }
}
