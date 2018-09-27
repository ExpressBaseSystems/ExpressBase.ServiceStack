using ExpressBase.Common;
using ExpressBase.Common.Constants;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Objects;
using ExpressBase.Common.Structures;
using ExpressBase.Objects;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ServiceStack;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;

namespace ExpressBase.ServiceStack.Services
{
    public class WebFormServices : EbBaseService
	{
		public WebFormServices(IEbConnectionFactory _dbf) : base(_dbf) { }

		//===================================== TABLE CREATION  ==========================================

		public CreateWebFormTableResponse Any(CreateWebFormTableRequest request)
		{
			return CreateWebFormTableRec(request);
		}

		private CreateWebFormTableResponse CreateWebFormTableRec(CreateWebFormTableRequest request)
		{
			foreach (EbControl _control in request.WebObj.Controls)
			{
				if (_control is EbControlContainer)
				{
					EbControlContainer Container = _control as EbControlContainer;
					Container.TableName = Container.TableName.IsNullOrEmpty() ? request.WebObj.TableName : Container.TableName;
					request.WebObj = Container;
					CreateWebFormTableResponse Response = CreateWebFormTableHelper(request);
					CreateWebFormTableRec(request);
				}
			}

			return new CreateWebFormTableResponse();
		}

		private CreateWebFormTableResponse CreateWebFormTableHelper(CreateWebFormTableRequest request)
		{
			IVendorDbTypes vDbTypes = this.EbConnectionFactory.ObjectsDB.VendorDbTypes;
			List<TableColumnMeta> _listNamesAndTypes = new List<TableColumnMeta>();

			IEnumerable<EbControl> _flatControls = request.WebObj.Controls.Get1stLvlControls();

			foreach (EbControl control in _flatControls)
			{
				//this.addControlToColl(control, ref _listNamesAndTypes, vDbTypes);
				_listNamesAndTypes.Add(new TableColumnMeta { Name = control.Name, Type = control.GetvDbType(vDbTypes) });
			}

			_listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_by", Type = vDbTypes.Decimal });
			_listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_aid", Type = vDbTypes.Decimal });
			_listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_created_at", Type = vDbTypes.DateTime });
			_listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_by", Type = vDbTypes.Decimal });
			_listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_aid", Type = vDbTypes.Decimal });
			_listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_lastmodified_at", Type = vDbTypes.DateTime });
			_listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_del", Type = vDbTypes.Boolean, Default = "F" });
			_listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_void", Type = vDbTypes.Boolean, Default = "F" });
			_listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_transaction_date", Type = vDbTypes.DateTime });
			_listNamesAndTypes.Add(new TableColumnMeta { Name = "eb_autogen", Type = vDbTypes.Decimal });

			CreateOrAlterTable(request.WebObj.TableName.ToLower(), _listNamesAndTypes);

			return new CreateWebFormTableResponse();
		}

		private int CreateOrAlterTable(string tableName, List<TableColumnMeta> listNamesAndTypes)
		{
			//checking for space in column name, table name
			foreach (TableColumnMeta entry in listNamesAndTypes)
			{
				if (entry.Name.Contains(CharConstants.SPACE) || tableName.Contains(CharConstants.SPACE))
					return -1;
			}
			var isTableExists = this.EbConnectionFactory.ObjectsDB.IsTableExists(this.EbConnectionFactory.ObjectsDB.IS_TABLE_EXIST, new DbParameter[] { this.EbConnectionFactory.ObjectsDB.GetNewParameter("tbl", EbDbTypes.String, tableName) });
			if (!isTableExists)
			{
				string cols = string.Join(CharConstants.COMMA + CharConstants.SPACE.ToString(), listNamesAndTypes.Select(x => x.Name + CharConstants.SPACE + x.Type.VDbType.ToString() + (x.Default.IsNullOrEmpty() ? "" : (" DEFAULT '" + x.Default + "'"))).ToArray());
				string sql = string.Empty;
				if (this.EbConnectionFactory.DataDB.Vendor == DatabaseVendors.ORACLE)////////////
				{
					sql = "CREATE TABLE @tbl(id NUMBER(10), @cols)".Replace("@cols", cols).Replace("@tbl", tableName);
					this.EbConnectionFactory.ObjectsDB.CreateTable(sql);//Table Creation
					CreateSquenceAndTrigger(tableName);//
				}
				else
				{
					sql = "CREATE TABLE @tbl( id SERIAL PRIMARY KEY, @cols)".Replace("@cols", cols).Replace("@tbl", tableName);
					this.EbConnectionFactory.ObjectsDB.CreateTable(sql);
				}
				return 0;
			}
			else
			{
				var colSchema = this.EbConnectionFactory.ObjectsDB.GetColumnSchema(tableName);
				string sql = string.Empty;
				foreach (TableColumnMeta entry in listNamesAndTypes)
				{
					bool isFound = false;
					foreach (EbDataColumn dr in colSchema)
					{
						if (entry.Name.ToLower() == (dr.ColumnName.ToLower()))
						{
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
							this.EbConnectionFactory.ObjectsDB.UpdateTable(sql);
							if (appendId)
								CreateSquenceAndTrigger(tableName);
						}
					}
					else
					{
						sql = (appendId ? "id SERIAL," : "") + sql;
						if (!sql.IsEmpty())
						{
							sql = "ALTER TABLE @tbl ADD COLUMN " + (sql.Substring(0, sql.Length - 1)).Replace(",", ", ADD COLUMN ");
							sql = sql.Replace("@tbl", tableName);
							this.EbConnectionFactory.ObjectsDB.UpdateTable(sql);
						}
					}
					return (0);
				}
			}
			return -1;
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
			this.EbConnectionFactory.ObjectsDB.CreateTable(sqnceSql);//Sequence Creation
			this.EbConnectionFactory.ObjectsDB.CreateTable(trgrSql);//Trigger Creation
		}


		//================================== GET PARTICULAR RECORD ================================================

		public EbDataSet Any(GetRowDataRequest request)
		{
			EbWebForm FormObj = GetWebFormObject(request.RefId);
			FormObj.TableRowId = request.RowId;
			string query = FormObj.GetQuery();
			EbDataSet dataset = this.EbConnectionFactory.ObjectsDB.DoQueries(query);

			return dataset;
		}

		private EbWebForm GetWebFormObject(string RefId)
		{
			var myService = base.ResolveService<EbObjectService>();
			var formObj = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = RefId });
			return EbSerializers.Json_Deserialize(formObj.Data[0].Json);
		}

        //======================================= SAVE OR UPDATE RECORD =============================================

        public InsertDataFromWebformResponse Any(InsertDataFromWebformRequest request)
        {
            EbObjectService myService = base.ResolveService<EbObjectService>();
            EbObjectParticularVersionResponse formObj = (EbObjectParticularVersionResponse)myService.Get(new EbObjectParticularVersionRequest() { RefId = request.RefId });
            EbWebForm FormObj = EbSerializers.Json_Deserialize(formObj.Data[0].Json);


            return new InsertDataFromWebformResponse();
        }

    }
}
