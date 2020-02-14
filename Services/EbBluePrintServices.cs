using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.Extensions;
using ExpressBase.Common.Structures;
using ExpressBase.Objects.ServiceStack_Artifacts;
using System;
using System.Collections.Generic;
using System.Data.Common;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

namespace ExpressBase.ServiceStack.Services
{
	
	public class EbBluePrintServices : EbBaseService
	{
		public EbBluePrintServices(IEbConnectionFactory _dbf) : base(_dbf) { }

		public SaveBluePrintResponse Post(SaveBluePrintRequest svgreq)
		{
			SaveBluePrintResponse svgres = new SaveBluePrintResponse();
			try
			{

					string sql = @"INSERT INTO  eb_blueprint(svgtext,bgimg_bytea,bp_meta)
								VALUES(
										:svgst,
										:imgbytea,
										:bpmeta
										)RETURNING id;";
					
					DbParameter[] parameters = {
									this.EbConnectionFactory.DataDB.GetNewParameter("svgst", EbDbTypes.String, svgreq.Txtsvg),
									this.EbConnectionFactory.DataDB.GetNewParameter("imgbytea", EbDbTypes.Bytea, svgreq.BgFile),
									this.EbConnectionFactory.DataDB.GetNewParameter("bpmeta", EbDbTypes.String, svgreq.MetaBluePrint)
									};
					
					EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(sql, parameters);
					svgres.Bprntid = Convert.ToInt32(dt.Rows[0][0]);
				
				
			}
			catch (Exception e)
			{
				Console.WriteLine("Exception: " + e.Message + e.StackTrace);
			}
			return svgres;
		}

		public RetriveBluePrintResponse Post(RetriveBluePrintRequest rtsvg)
		{
			RetriveBluePrintResponse rsv = new RetriveBluePrintResponse();
			try
			{

				string sql1 = @"SELECT 
									svgtext,
									bgimg_bytea,
									bp_meta
								FROM 
									eb_blueprint 
								WHERE
									id=:svgid";
				DbParameter[] param =
				{
					this.EbConnectionFactory.DataDB.GetNewParameter("svgid", EbDbTypes.Int32, rtsvg.Idno)
				};
				EbDataTable dt = this.EbConnectionFactory.DataDB.DoQuery(sql1, param);
				if (dt.Rows.Count > 0)
				{
					rsv.SvgPolyData = dt.Rows[0][0].ToString();
					var fileBase64Data = Convert.ToBase64String((byte[])(dt.Rows[0][1]));
					//rsv.FileDataURL = fileBase64Data;
					rsv.FileDataURL = string.Format("data:image/png;base64,{0}", fileBase64Data);
					rsv.BpMeta = dt.Rows[0][2].ToString();
				}
			}
			catch (Exception e)
			{
				Console.WriteLine("Exception: " + e.Message + e.StackTrace);
			}

			return rsv;
		}

		public  UpdateBluePrint_DevResponse Post(UpdateBluePrint_DevRequest upblresp) {
			UpdateBluePrint_DevResponse upblreq = new UpdateBluePrint_DevResponse();

			string tem = string.Empty;
			List<DbParameter> p = new List<DbParameter>();
			try
			{
				if (upblresp.BluePrintID > 0)
				{
					if (upblresp.BP_FormData_Dict.Count > 0)
					{
						foreach (var dct in upblresp.BP_FormData_Dict)
						{
							tem += dct.Key + "=" + ":" + dct.Key + ",";
							p.Add(this.EbConnectionFactory.DataDB.GetNewParameter(":" + dct.Key, EbDbTypes.String, dct.Value));
						}

						tem = tem.Remove(tem.Length - 1, 1);
						string sql = String.Format(@"UPDATE 
										eb_blueprint 
										SET
										{0}
										WHERE 
											id=:bpid", tem
													);

						p.Add(this.EbConnectionFactory.DataDB.GetNewParameter("bpid", EbDbTypes.Int32, upblresp.BluePrintID));
						DbParameter[] parameters = p.ToArray();
						int dt = this.EbConnectionFactory.DataDB.DoNonQuery(sql, parameters);
						if (dt > 0)
						{
							upblreq.Bprntid = upblresp.BluePrintID;
						}
					}
				}
			}
			catch (Exception e)
			{
				Console.WriteLine("Exception: " + e.Message + e.StackTrace);
			}
			return upblreq;
		}

			


	}
}
