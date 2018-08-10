using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
	public class LeadManagementService : EbBaseService
	{
		public LeadManagementService(IEbConnectionFactory _dbf) : base(_dbf) { }

		public GetManageLeadResponse Any(GetManageLeadRequest request)
		{
			string SqlQry = "SELECT firmcode, fname FROM firmmaster;";
			List<DbParameter> paramList = new List<DbParameter>();
			Dictionary<int, string> CostCenter = new Dictionary<int, string>();
			Dictionary<string, string> CustomerData = new Dictionary<string, string>();
			if (request.RequestMode == 1)//edit mode 
			{
				SqlQry += @"SELECT accountcode, firmcode, trdate, genurl, name, dob, age, genphoffice, profession, genemail,
							customertype, clcity, clcountry, city, typeofcustomer, sourcecategory, subcategory, consultation, picsrcvd
							FROM customervendor WHERE accountcode = :accountcode AND prehead='50';";
				paramList.Add(this.EbConnectionFactory.DataDB.GetNewParameter("accountcode", EbDbTypes.String, request.AccId));
			}			
			var ds = this.EbConnectionFactory.DataDB.DoQueries(SqlQry, paramList.ToArray());			
			foreach (var dr in ds.Tables[0].Rows)
			{
				CostCenter.Add(Convert.ToInt32(dr[0]), dr[1].ToString());
			}			
			if (ds.Tables.Count > 1 && ds.Tables[1].Rows.Count > 0)
			{
				var dr = ds.Tables[1].Rows[0];
				CustomerData.Add("accountcode", dr[0].ToString());
				CustomerData.Add("firmcode", dr[1].ToString());
				CustomerData.Add("trdate", Convert.ToDateTime(dr[2]).ToString("dd-MM-yyyy"));
				CustomerData.Add("genurl", dr[3].ToString());
				CustomerData.Add("name", dr[4].ToString());
				CustomerData.Add("dob", Convert.ToDateTime(dr[5]).ToString("dd-MM-yyyy"));
				CustomerData.Add("age", dr[6].ToString());
				CustomerData.Add("genphoffice", dr[7].ToString());
				CustomerData.Add("profession", dr[8].ToString());
				CustomerData.Add("genemail", dr[9].ToString());
				CustomerData.Add("customertype", dr[10].ToString());
				CustomerData.Add("clcity", dr[11].ToString());
				CustomerData.Add("clcountry", dr[12].ToString());
				CustomerData.Add("city", dr[13].ToString());
				CustomerData.Add("typeofcustomer", dr[14].ToString());
				CustomerData.Add("sourcecategory", dr[15].ToString());
				CustomerData.Add("subcategory", dr[16].ToString());
				CustomerData.Add("consultation", dr[17].ToString());
				CustomerData.Add("picsrcvd", dr[18].ToString());
			}			
			return new GetManageLeadResponse { CostCenterDict = CostCenter, CustomerDataDict = CustomerData };
		}

		public SaveCustomerResponse Any(SaveCustomerRequest request)
		{
			List<KeyValueType_Field> Fields = JsonConvert.DeserializeObject<List<KeyValueType_Field>>(request.CustomerData);
			var dict = Fields.ToDictionary(x => x.Key);
			KeyValueType_Field found;
			if (dict.TryGetValue("firmcode", out found)) { found.Type = EbDbTypes.Int32; found.Value = Convert.ToInt32(found.Value); }
			if (dict.TryGetValue("trdate", out found)) { found.Type = EbDbTypes.Date; found.Value = Convert.ToDateTime(found.Value); }
			if (dict.TryGetValue("genurl", out found)) { found.Type = EbDbTypes.String; }
			if (dict.TryGetValue("name", out found)) found.Type = EbDbTypes.String;
			if (dict.TryGetValue("dob", out found)) { found.Type = EbDbTypes.Date; found.Value = Convert.ToDateTime(found.Value); }
			if (dict.TryGetValue("age", out found)) found.Type = EbDbTypes.String;
			if (dict.TryGetValue("genphoffice", out found)) found.Type = EbDbTypes.String;
			if (dict.TryGetValue("profession", out found)) found.Type = EbDbTypes.String;
			if (dict.TryGetValue("genemail", out found)) found.Type = EbDbTypes.String;
			if (dict.TryGetValue("customertype", out found)) { found.Type = EbDbTypes.Int32; found.Value = Convert.ToInt32(found.Value); }
			if (dict.TryGetValue("clcity", out found)) found.Type = EbDbTypes.String;
			if (dict.TryGetValue("clcountry", out found)) found.Type = EbDbTypes.String;
			if (dict.TryGetValue("city", out found)) found.Type = EbDbTypes.String;
			if (dict.TryGetValue("typeofcustomer", out found)) found.Type = EbDbTypes.String;
			if (dict.TryGetValue("sourcecategory", out found)) found.Type = EbDbTypes.String;
			if (dict.TryGetValue("subcategory", out found)) found.Type = EbDbTypes.String;
			if (dict.TryGetValue("consultation", out found)) found.Type = EbDbTypes.String;
			if (dict.TryGetValue("picsrcvd", out found)) found.Type = EbDbTypes.String;

			int rstatus = 0;
			if (request.RequestMode == 0)//New Customer
			{
				Fields.Add(new KeyValueType_Field { Key = "prehead", Value = "50", Type = EbDbTypes.String });
				Fields.Add(new KeyValueType_Field { Key = "accountcode", Value = Fields.Find(i => i.Key == "genurl").Value, Type = EbDbTypes.String });
				rstatus = InsertToTable("customervendor", Fields);
			}			
			else if (request.RequestMode == 1)
			{
				List<KeyValueType_Field> WhereFields = new List<KeyValueType_Field>();
				WhereFields.Add(new KeyValueType_Field { Key = "prehead", Value = "50", Type = EbDbTypes.String });
				WhereFields.Add(new KeyValueType_Field { Key = "accountcode", Value = Fields.Find(i => i.Key == "genurl").Value, Type = EbDbTypes.String });
				rstatus = UpdateToTable("customervendor", Fields, WhereFields);
			}

			return new SaveCustomerResponse { Status = rstatus };
		}

		private int InsertToTable(string TblName, List<KeyValueType_Field> Fields)
		{
			List<DbParameter> parameters = new List<DbParameter>();
			string cols = string.Empty;
			string vals = string.Empty;
			foreach (KeyValueType_Field item in Fields)
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(item.Key, item.Type, item.Value));
				cols += item.Key + ",";
				vals += ":" + item.Key + ",";
			}
			string strQry = @"INSERT INTO @tblname@(@cols@) VALUES(@vals@);"
								.Replace("@tblname@", TblName)
								.Replace("@cols@", cols.Substring(0, cols.Length - 1))
								.Replace("@vals@", vals.Substring(0, vals.Length - 1));
			this.EbConnectionFactory.ObjectsDB.InsertTable(strQry, parameters.ToArray());
			return 1;
		}

		private int UpdateToTable(string TblName, List<KeyValueType_Field> Fields, List<KeyValueType_Field> WhereFields)
		{
			List<DbParameter> parameters = new List<DbParameter>();
			string s_set = string.Empty;
			string s_where = string.Empty;
			foreach (KeyValueType_Field item in Fields)
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(item.Key, item.Type, item.Value));
				s_set += item.Key + "=:" + item.Key + ",";
			}
			foreach (KeyValueType_Field item in WhereFields)
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(item.Key, item.Type, item.Value));
				s_where += item.Key + "=:" + item.Key + " AND ";
			}
			string strQry = @"UPDATE @tblname@ SET @str_set@ WHERE @str_where@;"
								.Replace("@tblname@", TblName)
								.Replace("@str_set@", s_set.Substring(0, s_set.Length - 1))
								.Replace("@str_where@", s_where.Substring(0, s_where.Length - 4));
			this.EbConnectionFactory.ObjectsDB.UpdateTable(strQry, parameters.ToArray());
			return 1;
		}


	}
}
