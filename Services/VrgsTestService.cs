//using ExpressBase.Common;
//using ExpressBase.Common.Data;
//using ExpressBase.Common.Extensions;
//using ExpressBase.Common.Structures;
//using ExpressBase.Objects.ServiceStack_Artifacts;
//using System;
//using System.Collections.Generic;
//using System.Data.Common;
//using System.IO;
//using System.Linq;
//using System.Threading.Tasks;

//namespace ExpressBase.ServiceStack.Services
//{
//	public class VrgsTestService : EbBaseService
//	{
//		public VrgsTestService(IEbConnectionFactory _dbf) : base(_dbf) { }

//		public SaveSvgResponse Post(SaveSvgRequest svgreq)
//		{
//			SaveSvgResponse svgres = new SaveSvgResponse();
//			try
//			{
//				List<DbParameter> Paramtr = new List<DbParameter>();
//				string[] ColArray = new string[] { "svgstring", "bgimg_bytea" };

//				string sql = @"INSERT INTO  vrgs_floorplan(svgstring,bgimg_bytea)
//							VALUES(
//									:svgst,
//									:imgbytea
//									)RETURNING id;";
//				//string sql = @"INSERT INTO  vrgs_floorplan({0})
//				//			VALUES(
//				//					:svgst,
//				//					:imgbytea
//				//					)RETURNING id;";
//				DbParameter[] parameters = {
//								this.InfraConnectionFactory.DataDB.GetNewParameter("svgst", EbDbTypes.String, svgreq.Txtsvg),
//								this.InfraConnectionFactory.DataDB.GetNewParameter("imgbytea", EbDbTypes.Bytea, svgreq.BgFile)
//								};
//				//Paramtr.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("svgst", EbDbTypes.String, svgreq.Txtsvg));
//				//Paramtr.Add(this.InfraConnectionFactory.DataDB.GetNewParameter("imgbytea", EbDbTypes.Bytea, svgreq.BgFile));
//				//DbParameter[] parameters = Paramtr.ToArray();
//				EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql, parameters);
//				var rslt = Convert.ToInt32(dt.Rows[0][0]);
//			}
//			catch (Exception e)
//			{
//				Console.WriteLine("Exception: " + e.Message + e.StackTrace);
//			}
//			return svgres;
//		}

//		public RetriveSVGResponse post(RetriveSVGRequest rtsvg)
//		{
//			RetriveSVGResponse rsv = new RetriveSVGResponse();
//			try
//			{

//				string sql1 = @"SELECT 
//									svgstring,
//									bgimg_bytea
//								FROM 
//									vrgs_floorplan 
//								WHERE
//									id=:svgid";
//				DbParameter[] param =
//				{
//					this.InfraConnectionFactory.DataDB.GetNewParameter("svgid", EbDbTypes.Int32, rtsvg.Idno)
//				};
//				EbDataTable dt = this.InfraConnectionFactory.DataDB.DoQuery(sql1, param);
//				if (dt.Rows.Count > 0)
//				{
//					rsv.SvgPolyData = dt.Rows[0][0].ToString();
//					var fileBase64Data = Convert.ToBase64String((byte[])(dt.Rows[0][1]));
//					//rsv.FileDataURL = fileBase64Data;
//					rsv.FileDataURL = string.Format("data:image/png;base64,{0}", fileBase64Data);

//				}
//			}
//			catch (Exception e)
//			{
//				Console.WriteLine("Exception: " + e.Message + e.StackTrace);
//			}

//			return rsv;
//		}




//	}
//}
