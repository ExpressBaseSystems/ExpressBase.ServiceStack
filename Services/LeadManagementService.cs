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
			Dictionary<int, string> CostCenter = new Dictionary<int, string>();
			CostCenter.Add(0, "Hair o Craft");
			CostCenter.Add(1, "HOC Kochi");
			CostCenter.Add(2, "HOC Kozhikode");
			CostCenter.Add(3, "HOC TRIVANDRUM");
			CostCenter.Add(4, "HOC Coimbatore");

			Dictionary<string, string> CustomerData = new Dictionary<string, string>();
			CustomerData.Add("AccId","0");

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
				s_set += item.Key + "=" + item.Value + ",";
			}
			foreach (KeyValueType_Field item in WhereFields)
			{
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(item.Key, item.Type, item.Value));
				s_where += item.Key + "=" + item.Value + ",";
			}
			string strQry = @"UPDATE @tblname@ SET @str_set@ WHERE @str_where@;"
								.Replace("@tblname@", TblName)
								.Replace("@str_set@", s_set.Substring(0, s_set.Length - 1))
								.Replace("@str_where@", s_where.Substring(0, s_where.Length - 1));
			this.EbConnectionFactory.ObjectsDB.UpdateTable(strQry, parameters.ToArray());
			return 1;
		}


	}
}
