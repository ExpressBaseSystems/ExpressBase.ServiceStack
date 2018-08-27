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
			List<FeedbackEntry> Flist = new List<FeedbackEntry>();
			List<BillingEntry> Blist = new List<BillingEntry>();
			List<SurgeryEntry> Slist = new List<SurgeryEntry>();
			if (request.RequestMode == 1)//edit mode 
			{
				SqlQry += @"SELECT accountcode, firmcode, trdate, genurl, name, dob, age, genphoffice, profession, genemail,
								customertype, clcity, clcountry, city, typeofcustomer, sourcecategory, subcategory, consultation, picsrcvd
								FROM customervendor WHERE accountcode = :accountcode AND prehead='50';
							SELECT id,trdate,status,followupdate,narration, createdby FROM leaddetails
								WHERE accountcode=:accountcode AND prehead='50' ORDER BY trdate DESC;
							SELECT id,trdate,totalamount,advanceamount,balanceamount,cashreceived,paymentmode,bank,createddt,narration,createdby 
								FROM leadpaymentdetails WHERE accountcode=:accountcode ORDER BY trdate DESC;
							SELECT id,dateofsurgery,branch,patientinstructions,doctorsinstructions,createdby,createddt 
								FROM leadsurgerydetails WHERE accountcode=:accountcode AND prehead='50';";
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

				//followup details
				foreach (var i in ds.Tables[2].Rows)
				{
					Flist.Add(new FeedbackEntry
					{
						Id = Convert.ToInt32(i[0]),
						Date = Convert.ToDateTime(i[1]).ToString("dd-MM-yyyy"),
						Status = i[2].ToString(),
						Followup_Date = Convert.ToDateTime(i[3]).ToString("dd-MM-yyyy"),						
						Comments = i[4].ToString(),
						Created_By = i[5].ToString()
					});
				}

				//Billing details
				foreach (var i in ds.Tables[3].Rows)
				{
					Blist.Add(new BillingEntry
					{
						Id = Convert.ToInt32(i[0]),
						Date = Convert.ToDateTime(i[1]).ToString("dd-MM-yyyy"),
						Total_Amount = Convert.ToInt32(i[2]),
						Amount_Received = Convert.ToInt32(i[3]),
						Balance_Amount = Convert.ToInt32(i[4]),
						Cash_Paid = Convert.ToInt32(i[5]),
						Payment_Mode = i[6].ToString(),
						Bank = i[7].ToString(),
						Clearence_Date = Convert.ToDateTime(i[8]).ToString("dd-MM-yyyy"),
						Narration = i[9].ToString(),
						Created_By = i[10].ToString()
					});
				}

				//surgery details
				foreach (var i in ds.Tables[4].Rows)
				{
					Slist.Add(new SurgeryEntry
					{
						Id = Convert.ToInt32(i[0]),
						Date = Convert.ToDateTime(i[1]).ToString("dd-MM-yyyy"),
						Branch = i[2].ToString(),
						Created_By = i[5].ToString(),
						Created_Date = i[6].ToString()
					});
				}
			}


			return new GetManageLeadResponse { CostCenterDict = CostCenter, CustomerDataDict = CustomerData, FeedbackList = Flist, BillingList = Blist, SurgeryList = Slist };
		}

		public SaveCustomerResponse Any(SaveCustomerRequest request)
		{
			List<KeyValueType_Field> Fields = JsonConvert.DeserializeObject<List<KeyValueType_Field>>(request.CustomerData);
			var dict = Fields.ToDictionary(x => x.Key);
			KeyValueType_Field found;
			List<DbParameter> parameters = new List<DbParameter>();
			if (dict.TryGetValue("firmcode", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value))); 
			if (dict.TryGetValue("trdate", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Date, Convert.ToDateTime(found.Value)));
			if (dict.TryGetValue("genurl", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
			if (dict.TryGetValue("name", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
			if (dict.TryGetValue("dob", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Date, Convert.ToDateTime(found.Value)));
			if (dict.TryGetValue("age", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
			if (dict.TryGetValue("genphoffice", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
			if (dict.TryGetValue("profession", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
			if (dict.TryGetValue("genemail", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
			if (dict.TryGetValue("customertype", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.Int32, Convert.ToInt32(found.Value)));
			if (dict.TryGetValue("clcity", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
			if (dict.TryGetValue("clcountry", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
			if (dict.TryGetValue("city", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
			if (dict.TryGetValue("typeofcustomer", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
			if (dict.TryGetValue("sourcecategory", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
			if (dict.TryGetValue("subcategory", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
			if (dict.TryGetValue("consultation", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));
			if (dict.TryGetValue("picsrcvd", out found))
				parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(found.Key, EbDbTypes.String, found.Value));

			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("prehead", EbDbTypes.String, "50"));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountcode", EbDbTypes.String, Fields.Find(i => i.Key == "genurl").Value));

			int rstatus = 0;
			if (request.RequestMode == 0)//New Customer
			{
				string Qry = @"INSERT INTO customervendor(firmcode, trdate, genurl, name, dob, age, genphoffice, profession, genemail, customertype, clcity, clcountry, city, typeofcustomer, sourcecategory, subcategory, consultation, picsrcvd) 
										VALUES(:firmcode, :trdate, :genurl, :name, :dob, :age, :genphoffice, :profession, :genemail, :customertype, :clcity, :clcountry, :city, :typeofcustomer, :sourcecategory, :subcategory, :consultation, :picsrcvd);";
				rstatus = this.EbConnectionFactory.ObjectsDB.InsertTable(Qry, parameters.ToArray());
			}			
			else if (request.RequestMode == 1)
			{
				string Qry = @"UPDATE customervendor 
								SET genphoffice=:genphoffice, profession=:profession, genemail=:genemail, customertype=:customertype, clcity=:clcity, clcountry=:clcountry, city=:city, typeofcustomer=:typeofcustomer, sourcecategory=:sourcecategory, subcategory=:subcategory, consultation=:consultation, picsrcv=:picsrcv 
								WHERE prehead = :prehead AND accountcode = :accountcode;";
				rstatus = this.EbConnectionFactory.ObjectsDB.UpdateTable(Qry, parameters.ToArray());
			}
			return new SaveCustomerResponse { Status = rstatus };
		}

		public SaveCustomerFollowupResponse Any(SaveCustomerFollowupRequest request)
		{
			int rstatus = 0;
			FeedbackEntry F_Obj = JsonConvert.DeserializeObject<FeedbackEntry>(request.Data);
			List<DbParameter> parameters = new List<DbParameter>();
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, F_Obj.Id));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountcode", EbDbTypes.String, F_Obj.Account_Code));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("trdate", EbDbTypes.String, F_Obj.Date));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("status", EbDbTypes.String, F_Obj.Status));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("followupdate", EbDbTypes.String, F_Obj.Followup_Date));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("narration", EbDbTypes.String, F_Obj.Comments));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createddt", EbDbTypes.String, DateTime.Now.ToString("dd-MM-yyyy")));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifiedby", EbDbTypes.String, request.UserName));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifieddt", EbDbTypes.String, DateTime.Now.ToString("dd-MM-yyyy")));

			if (F_Obj.Id == 0)//new
			{
				string Qry = @"INSERT INTO leaddetails(prehead, accountcode, trdate, status, followupdate, narration, createdby, createddt) 
									VALUES('50 , :accountcode, :trdate, :status, :followupdate, :narration, :createdby, :createddt);";
				rstatus = this.EbConnectionFactory.ObjectsDB.InsertTable(Qry, parameters.ToArray());
			}
			else//update
			{
				string Qry = @"UPDATE leaddetails 
								SET status=:status, followupdate=:followupdate, narration=:narration, modifiedby = :modifiedby, modifieddt = :modifieddt  
								WHERE prehead = '50' AND accountcode = :accountcode AND id=:id;";
				rstatus = this.EbConnectionFactory.ObjectsDB.UpdateTable(Qry, parameters.ToArray());
			}
			return new SaveCustomerFollowupResponse { Status = rstatus };
		}

		public SaveCustomerPaymentResponse Any(SaveCustomerPaymentRequest request)
		{
			int rstatus = 0;
			BillingEntry B_Obj = JsonConvert.DeserializeObject<BillingEntry>(request.Data);
			List<DbParameter> parameters = new List<DbParameter>();
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, B_Obj.Id));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountcode", EbDbTypes.String, B_Obj.Account_Code));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("trdate", EbDbTypes.String, B_Obj.Date));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("totalamount", EbDbTypes.String, B_Obj.Total_Amount));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("advanceamount", EbDbTypes.String, B_Obj.Amount_Received));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("paymentmode", EbDbTypes.String, B_Obj.Payment_Mode));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("bank", EbDbTypes.String, B_Obj.Bank));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("balanceamount", EbDbTypes.String, B_Obj.Balance_Amount));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("cashreceived", EbDbTypes.String, B_Obj.Amount_Received));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createddt", EbDbTypes.String, DateTime.Now.ToString("dd-MM-yyyy")));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("narration", EbDbTypes.String, B_Obj.Narration));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifiedby", EbDbTypes.String, request.UserName));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifieddt", EbDbTypes.String, DateTime.Now.ToString("dd-MM-yyyy")));
			if (B_Obj.Id == 0)//new
			{
				string Qry = @"INSERT INTO leadpaymentdetails(prehead,accountcode,trdate,totalamount,advanceamount,paymentmode,bank,balanceamount,cashreceived,createdby,createddt,narration) 
													VALUES (50,:accountcode,:trdate,:totalamount,:advanceamount,:paymentmode,:bank,:balanceamount,:cashreceived,:createdby,:createddt,:narration);";
				rstatus = this.EbConnectionFactory.ObjectsDB.InsertTable(Qry, parameters.ToArray());
			}
			else//update
			{
				string Qry = @"UPDATE leadpaymentdetails 
								SET paymentmode = :paymentmode, bank = :bank, balanceamount = :balanceamount, cashreceived = :cashreceived,
									narration = :narration, modifiedby = :modifiedby, modifieddt = :modifieddt 
								WHERE accountcode=:accountcode AND id = :id;";
				rstatus = this.EbConnectionFactory.ObjectsDB.UpdateTable(Qry, parameters.ToArray());
			}
			return new SaveCustomerPaymentResponse { Status = rstatus };
		}

		public SaveSurgeryDetailsResponse Any(SaveSurgeryDetailsRequest request)
		{
			int rstatus = 0;
			SurgeryEntry S_Obj = JsonConvert.DeserializeObject<SurgeryEntry>(request.Data);
			List<DbParameter> parameters = new List<DbParameter>();
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("id", EbDbTypes.Int32, S_Obj.Id));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("accountcode", EbDbTypes.String, S_Obj.Account_Code));

			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("dateofsurgery", EbDbTypes.String, S_Obj.Date));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("branch", EbDbTypes.String, S_Obj.Branch));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("patientinstructions", EbDbTypes.String, ""));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("doctorinstructions", EbDbTypes.String, ""));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createdby", EbDbTypes.String, request.UserName));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("createddt", EbDbTypes.String, DateTime.Now.ToString("dd-MM-yyyy")));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifiedby", EbDbTypes.String, request.UserName));
			parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter("modifieddt", EbDbTypes.String, DateTime.Now.ToString("dd-MM-yyyy")));
			if (S_Obj.Id == 0)//new
			{
				string Qry = @"INSERT INTO leadsurgerydetals(prehead,accountcode,dateofsurgery,branch,patientinstructions,doctorinstructions,createdby,createddt)
													VALUES (50,:accountcode,:dateofsurgery,:branch,:patientinstructions,:doctorinstructions,:createdby,:createddt);";
				rstatus = this.EbConnectionFactory.ObjectsDB.InsertTable(Qry, parameters.ToArray());
			}
			else//update
			{
				string Qry = @"UPDATE leadsurgerydetails 
								SET dateofsurgery = :dateofsurgery,branch = :branch,patientinstructions = :patientinstructions,doctorinstructions = :doctorinstructions,
									createdby = :createdby,createddt = :createddt, modifiedby = :modifiedby, modifieddt = :modifieddt 
								WHERE accountcode=:accountcode AND id = :id;";
				rstatus = this.EbConnectionFactory.ObjectsDB.UpdateTable(Qry, parameters.ToArray());
			}
			return new SaveSurgeryDetailsResponse { Status = rstatus };
		}






		//private int InsertToTable(string TblName, List<KeyValueType_Field> Fields)
		//{
		//	List<DbParameter> parameters = new List<DbParameter>();
		//	string cols = string.Empty;
		//	string vals = string.Empty;
		//	foreach (KeyValueType_Field item in Fields)
		//	{
		//		parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(item.Key, item.Type, item.Value));
		//		cols += item.Key + ",";
		//		vals += ":" + item.Key + ",";
		//	}
		//	string strQry = @"INSERT INTO @tblname@(@cols@) VALUES(@vals@);"
		//						.Replace("@tblname@", TblName)
		//						.Replace("@cols@", cols.Substring(0, cols.Length - 1))
		//						.Replace("@vals@", vals.Substring(0, vals.Length - 1));
		//	this.EbConnectionFactory.ObjectsDB.InsertTable(strQry, parameters.ToArray());
		//	return 1;
		//}

		//private int UpdateToTable(string TblName, List<KeyValueType_Field> Fields, List<KeyValueType_Field> WhereFields)
		//{
		//	List<DbParameter> parameters = new List<DbParameter>();
		//	string s_set = string.Empty;
		//	string s_where = string.Empty;
		//	foreach (KeyValueType_Field item in Fields)
		//	{
		//		parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(item.Key, item.Type, item.Value));
		//		s_set += item.Key + "=:" + item.Key + ",";
		//	}
		//	foreach (KeyValueType_Field item in WhereFields)
		//	{
		//		parameters.Add(this.EbConnectionFactory.ObjectsDB.GetNewParameter(item.Key, item.Type, item.Value));
		//		s_where += item.Key + "=:" + item.Key + " AND ";
		//	}
		//	string strQry = @"UPDATE @tblname@ SET @str_set@ WHERE @str_where@;"
		//						.Replace("@tblname@", TblName)
		//						.Replace("@str_set@", s_set.Substring(0, s_set.Length - 1))
		//						.Replace("@str_where@", s_where.Substring(0, s_where.Length - 4));
		//	this.EbConnectionFactory.ObjectsDB.UpdateTable(strQry, parameters.ToArray());
		//	return 1;
		//}
	}
}
