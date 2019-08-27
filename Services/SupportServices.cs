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
													type_bg_fr
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
													:typ
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
					this.InfraConnectionFactory.DataDB.GetNewParameter("typ", EbDbTypes.String,sbreq.type_b_f)
					};

				EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);
				sb.Id = Convert.ToInt32(dt.Rows[0][0]);

				if (sb.Id > 0)
				{
					string sbgf = null;
					if (sbreq.type_b_f.Equals("bug"))
					{
						sbgf = "B" + sb.Id;

					}
					string k = String.Format("UPDATE support_ticket SET bg_fr_id = :bfi WHERE id={0} and eb_del='F';",sb.Id );
					DbParameter[] param = {
					this.InfraConnectionFactory.DataDB.GetNewParameter("bfi", EbDbTypes.String,sbgf)
					};
					int dt2= this.InfraConnectionFactory.DataDB.DoNonQuery(k, param);
				}


			}
			catch(Exception e)
			{
				Console.WriteLine("Exception: " + e.Message + e.StackTrace);
			}
			return sb;
		}

		public TenantSolutionsResponse Post(TenantSolutionsRequest tsreq)
		{
			TenantSolutionsResponse tr = new TenantSolutionsResponse();

			try
			{
				string sql = string.Format("SELECT solution_id,solution_name  FROM eb_solutions WHERE tenant_id={0} AND eb_del='F';", tsreq.UserId);
				
					EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql);


				for (int i = 0; i < dt.Rows.Count; i++)
					{
						tr.solid.Add(dt.Rows[i][0].ToString());
						tr.solname.Add(dt.Rows[i][1].ToString());
					}
			}
			catch(Exception e)
			{
				Console.WriteLine("Exception: " + e.Message + e.StackTrace);
			}
			return tr;
		}


		public FetchSupportResponse Post(FetchSupportRequest fsreq)
		{
			FetchSupportResponse fr = new FetchSupportResponse();
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
								bg_fr_id
								FROM support_ticket
								WHERE 
								eb_created_by ={0} AND eb_del='F';", fsreq.UserId);

				EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql);
				for (int i = 0; i < dt.Rows.Count; i++)
				{
					SupportTktCls st = new SupportTktCls();
					st.title=dt.Rows[i][0].ToString();
					st.description=dt.Rows[i][1].ToString();
					st.priority=dt.Rows[i][2].ToString();
					st.solutionid=dt.Rows[i][3].ToString();
					st.lstmodified=dt.Rows[i][4].ToString();
					st.status=dt.Rows[i][5].ToString();
					st.remarks=dt.Rows[i][6].ToString();
					st.support=dt.Rows[i][7].ToString();
					st.type_b_f=dt.Rows[i][8].ToString();
					st.ticketid=dt.Rows[i][9].ToString();
					fr.supporttkt.Add(st);
				}
			}
			catch(Exception e)
			{
				Console.WriteLine("Excetion " + e.Message + e.StackTrace);
			}
			return fr;
		}
	}
}
