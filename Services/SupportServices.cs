using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
	public class SupportServices: EbBaseService
	{
		public SupportServices(IEbConnectionFactory _dbf) : base(_dbf) { }

		public SaveBugResponse Post(SaveBugRequest sbreq)
		{
			SaveBugResponse sb = new SaveBugResponse();
			try
			{
				string sql = @"INSERT INTO support_ticket(
                                                    eb_created_by,
                                                    user_type,
                                                    title,
                                                    description,
                                                    priority,
                                                    solution_id,
													eb_created_at,
													modified_at,
													eb_del,
													status,
													type_bg_fr,
													fullname,
													email
													)
													VALUES(
                                                    :usrid,
                                                    :usrtyp,
                                                    :title,
                                                    :descr,
                                                    :priority,
                                                    :solid,
													 NOW(),
													 NOW(),
													:fals,
													:sts,
													:typ,
													:fname,
													:email
                                                )RETURNING id;";

				DbParameter[] parameters = {
					this.InfraConnectionFactory.DataDB.GetNewParameter("usrid", EbDbTypes.Int32, sbreq.UserId),
					this.InfraConnectionFactory.DataDB.GetNewParameter("usrtyp", EbDbTypes.String, sbreq.usertype),
					this.InfraConnectionFactory.DataDB.GetNewParameter("title", EbDbTypes.String, sbreq.title),
					this.InfraConnectionFactory.DataDB.GetNewParameter("descr", EbDbTypes.String, sbreq.description),
					this.InfraConnectionFactory.DataDB.GetNewParameter("priority", EbDbTypes.String, sbreq.priority),
					this.InfraConnectionFactory.DataDB.GetNewParameter("solid", EbDbTypes.String, sbreq.solutionid),
					this.InfraConnectionFactory.DataDB.GetNewParameter("fals", EbDbTypes.String, "F"),
					this.InfraConnectionFactory.DataDB.GetNewParameter("sts", EbDbTypes.String, sbreq.status),
					this.InfraConnectionFactory.DataDB.GetNewParameter("typ", EbDbTypes.String,sbreq.type_b_f),
					this.InfraConnectionFactory.DataDB.GetNewParameter("fname", EbDbTypes.String,sbreq.fullname),
					this.InfraConnectionFactory.DataDB.GetNewParameter("email", EbDbTypes.String,sbreq.email)
					};

				EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);
				sb.Id = Convert.ToInt32(dt.Rows[0][0]);

				if (sb.Id > 0)
				{
					string cx = sb.Id.ToString();

					//for making id 6 digit with intial position 0 in case of single digit

					string l = string.Format("select lpad('{0}',6,'0');",cx );
					EbDataTable dt3 = this.InfraConnectionFactory.DataDB.DoQuery(l);
					string sbgf = null;
					if (sbreq.type_b_f.Equals("bug"))
					{
						sbgf = "IS" + dt3.Rows[0][0];

					}
					else if (sbreq.type_b_f.Equals("featurerequest"))
					{
						sbgf = "IS" + dt3.Rows[0][0];
					}
					string k = String.Format("UPDATE support_ticket SET bg_fr_id = :bfi WHERE id={0} and eb_del='F';",sb.Id );
					DbParameter[] param = {
					this.InfraConnectionFactory.DataDB.GetNewParameter("bfi", EbDbTypes.String,sbgf)
					};
					int dt2= this.InfraConnectionFactory.DataDB.DoNonQuery(k, param);

					//to upload images

					if (sbreq.upload_files.Count > 0)
					{

					}
				}



			}
			catch(Exception e)
			{
				Console.WriteLine("Exception: " + e.Message + e.StackTrace);
			}
			return sb;
		}



		//to fetch solution id,name from tenant table  to show in dropdown

		public TenantSolutionsResponse Post(TenantSolutionsRequest tsreq)
		{
			TenantSolutionsResponse tr = new TenantSolutionsResponse();

			try
			{
				string sql = string.Format("SELECT isolution_id,solution_name,esolution_id  FROM eb_solutions WHERE tenant_id={0} AND eb_del='F';", tsreq.UserId);
				
					EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql);


				for (int i = 0; i < dt.Rows.Count; i++)
					{
						tr.solid.Add(dt.Rows[i][0].ToString());
						tr.solname.Add(dt.Rows[i][1].ToString());
						tr.soldispid.Add(dt.Rows[i][2].ToString());
					}
			}
			catch(Exception e)
			{
				Console.WriteLine("Exception: " + e.Message + e.StackTrace);
			}
			return tr;
		}


		//to fetch all details of tickets of corresponding user of that corresponding solution to show as tables
		public FetchSupportResponse Post(FetchSupportRequest fsreq)
		{
			FetchSupportResponse fr = new FetchSupportResponse();
			try
			{
				
				if (fsreq.WhichConsole.Equals("tc"))
				{
					

					string sql2 = @"SELECT 
								title, 
								description,
								priority, 
								solution_id, 
								modified_at, 
								status, 
								remarks, 
								assigned_to, 
								type_bg_fr,
								bg_fr_id
								FROM support_ticket
								WHERE 
								eb_del='F' 
								AND 
								solution_id 
								IN
								(SELECT isolution_id  FROM eb_solutions WHERE tenant_id=:tndid AND eb_del='F');";
					DbParameter[] parameters2 = {
					this.InfraConnectionFactory.DataDB.GetNewParameter("tndid", EbDbTypes.Int32, fsreq.UserId)
					};

					EbDataTable dt2 = this.InfraConnectionFactory.DataDB.DoQuery(sql2, parameters2);
					for (int i = 0; i < dt2.Rows.Count; i++)
					{
						SupportTktCls st = new SupportTktCls();
						st.title = dt2.Rows[i][0].ToString();
						st.description = dt2.Rows[i][1].ToString();
						st.priority = dt2.Rows[i][2].ToString();
						st.solutionid = dt2.Rows[i][3].ToString();
						st.lstmodified = dt2.Rows[i][4].ToString();
						st.status = dt2.Rows[i][5].ToString();
						st.remarks = dt2.Rows[i][6].ToString();
						st.assignedto = dt2.Rows[i][7].ToString();
						st.type_b_f = dt2.Rows[i][8].ToString();
						st.ticketid = dt2.Rows[i][9].ToString();
						fr.supporttkt.Add(st);
					}
				}
				else
				{
					string sql = @"SELECT 
								title, 
								description,
								priority, 
								solution_id, 
								modified_at, 
								status, 
								remarks, 
								assigned_to, 
								type_bg_fr,
								bg_fr_id
								FROM support_ticket
								WHERE 
								eb_created_by =:usr AND solution_id =:sln AND eb_del=:fls;";
					DbParameter[] parameters3 = {
					this.InfraConnectionFactory.DataDB.GetNewParameter("usr", EbDbTypes.Int32, fsreq.UserId),
					this.InfraConnectionFactory.DataDB.GetNewParameter("sln", EbDbTypes.String, fsreq.SolnId),
					this.InfraConnectionFactory.DataDB.GetNewParameter("fls", EbDbTypes.String, "F")
					};

					EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters3);
					for (int i = 0; i < dt.Rows.Count; i++)
					{
						SupportTktCls st = new SupportTktCls();
						st.title = dt.Rows[i][0].ToString();
						st.description = dt.Rows[i][1].ToString();
						st.priority = dt.Rows[i][2].ToString();
						st.solutionid = dt.Rows[i][3].ToString();
						st.lstmodified = dt.Rows[i][4].ToString();
						st.status = dt.Rows[i][5].ToString();
						st.remarks = dt.Rows[i][6].ToString();
						st.assignedto = dt.Rows[i][7].ToString();
						st.type_b_f = dt.Rows[i][8].ToString();
						st.ticketid = dt.Rows[i][9].ToString();
						fr.supporttkt.Add(st);
					}
				}
				
			}
			catch(Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
			}
			return fr;
		}


		// fectch complete details of ticket and show it in edit /view ticket
		public SupportDetailsResponse Post(SupportDetailsRequest sdreq)
		{
			SupportDetailsResponse sd = new SupportDetailsResponse();
			try
			{
				string sql = string.Format(@"SELECT 
								title, 
								description,
								priority, 
								solution_id, 
								modified_at, 
								status, 
								remarks, 
								assigned_to, 
								type_bg_fr,
								eb_created_at
								FROM support_ticket
								WHERE 
								bg_fr_id ='{0}' AND eb_del='F';", sdreq.ticketno);

				EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql);
				for (int i = 0; i < dt.Rows.Count; i++)
				{
					SupportTktCls st = new SupportTktCls();
					st.title = dt.Rows[i][0].ToString();
					st.description = dt.Rows[i][1].ToString();
					st.priority = dt.Rows[i][2].ToString();
					st.solutionid = dt.Rows[i][3].ToString();
					st.lstmodified = dt.Rows[i][4].ToString();
					st.status = dt.Rows[i][5].ToString();
					st.remarks = dt.Rows[i][6].ToString();
					st.assignedto = dt.Rows[i][7].ToString();
					st.type_b_f = dt.Rows[i][8].ToString();
					st.createdat = dt.Rows[i][9].ToString();
					st.ticketid = sdreq.ticketno;
					sd.supporttkt.Add(st);
				}
			}
			catch (Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
			}
			return sd;
		}



		public UpdateTicketResponse Post(UpdateTicketRequest utreq)
		{
			UpdateTicketResponse utr = new UpdateTicketResponse();
			utr.status = false;
			try
			{
				string k = String.Format(@"UPDATE 
										support_ticket 
										SET
										title = :title, 
										description = :descr,
										priority = :prior
										WHERE 
											bg_fr_id=:tktid
                                            and eb_del='F'"
											);
				DbParameter[] parameters = {
					this.InfraConnectionFactory.DataDB.GetNewParameter("tktid", EbDbTypes.String, utreq.ticketid),
					this.InfraConnectionFactory.DataDB.GetNewParameter("title", EbDbTypes.String, utreq.title),
					this.InfraConnectionFactory.DataDB.GetNewParameter("description", EbDbTypes.String, utreq.description),
					this.InfraConnectionFactory.DataDB.GetNewParameter("priority", EbDbTypes.String, utreq.priority)
					};
				int dt = this.InfraConnectionFactory.DataDB.DoNonQuery(k, parameters);
				utr.status = true;
			}
			catch(Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
			}
			return utr;
		}

	}
}
