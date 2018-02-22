using ExpressBase.Common;
using ExpressBase.Common.Data;
using ExpressBase.Common.EbServiceStack.ReqNRes;
using ExpressBase.Objects;
using ExpressBase.Objects.ReportRelated;
using ExpressBase.Objects.ServiceStack_Artifacts;
using ExpressBase.ServiceStack.MQServices;
using iTextSharp.text;
using iTextSharp.text.pdf;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using QRCoder;
using Microsoft.CodeAnalysis.CSharp.Scripting;
using Microsoft.CodeAnalysis.Scripting;
using System.Dynamic;
using ExpressBase.Objects.Objects.ReportRelated;

namespace ExpressBase.ServiceStack
{
    public class ReportService : EbBaseService
    {
        private DataSourceColumnsResponse cresp = null;
        private DataSourceDataResponse dresp = null;

        public EbReport Report = null;
        //private iTextSharp.text.Font f = FontFactory.GetFont(FontFactory.HELVETICA, 12);
        public ReportService(IEbConnectionFactory _dbf) : base(_dbf) { }


        public ReportRenderResponse Get(ReportRenderRequest request)
        {
            //-- Get REPORT object and Init 
            var myObjectservice = base.ResolveService<EbObjectService>();
            EbObjectParticularVersionResponse resultlist = myObjectservice.Get(new EbObjectParticularVersionRequest { RefId = request.Refid }) as EbObjectParticularVersionResponse;
            Report = EbSerializers.Json_Deserialize<EbReport>(resultlist.Data[0].Json);
            Report.ReportService = this;
            Report.FileService = base.ResolveService<FileService>();
            Report.SolutionId = request.TenantAccountId;
            Report.IsLastpage = false;
            Report.watermarkImages = new Dictionary<string, byte[]>();
            Report.WaterMarkList = new List<object>();
            Report.ScriptCollection = new Dictionary<string, Script>();
            Report.CurrentTimestamp = DateTime.Now;
            //-- END REPORT object INIT

            var myDataSourceservice = base.ResolveService<DataSourceService>();
            if (Report.DataSourceRefId != string.Empty)
            {
                Console.WriteLine("Report.DataSourceRefId   :"+ Report.DataSourceRefId);
                   //    dresp = myDataSourceservice.Any(new DataSourceDataRequest444 { RefId = Report.DataSourceRefId, Draw = 1, Start = 0, Length = 100 });
                   //    Report.DataSet = dresp.DataSet;

                   cresp = this.Redis.Get<DataSourceColumnsResponse>(string.Format("{0}_columns", Report.DataSourceRefId));
                if (cresp== null)
                    cresp = myDataSourceservice.Any(new DataSourceColumnsRequest { RefId = Report.DataSourceRefId });
                Report.DataColumns = (cresp.Columns.Count > 1) ? cresp.Columns[1] : cresp.Columns[0];
                dresp = myDataSourceservice.Any(new DataSourceDataRequest { RefId = Report.DataSourceRefId, Draw = 1, Start = 0, Length = 100 });
                Report.DataRow = dresp.Data; Console.WriteLine("Rows: " + dresp.Data.Count);

            }

            Rectangle rec = new Rectangle(Report.Width, Report.Height);
            Report.Doc = new Document(rec);
            Report.Ms1 = new MemoryStream();
            Report.Writer = PdfWriter.GetInstance(Report.Doc, Report.Ms1);
            Report.Writer.Open();
            Report.Doc.Open();
            Report.Writer.PageEvent = new HeaderFooter(Report);
            Report.Writer.CloseStream = true;//important
            Report.Canvas = Report.Writer.DirectContent;
            Report.PageNumber = Report.Writer.PageNumber;
            Report.InitializeSummaryFields();
            GetWatermarkImages();

            foreach (EbReportHeader r_header in Report.ReportHeaders)
                this.FillScriptCollection(r_header.Fields);

            foreach (EbReportFooter r_footer in Report.ReportFooters)
                this.FillScriptCollection(r_footer.Fields);

            foreach (EbPageHeader p_header in Report.PageHeaders)
                this.FillScriptCollection(p_header.Fields);

            foreach (EbReportDetail detail in Report.Detail)
                this.FillScriptCollection(detail.Fields);

            foreach (EbPageFooter p_footer in Report.PageFooters)
                this.FillScriptCollection(p_footer.Fields);

            //Globals globals = new Globals();
            //foreach (string calcfd in (field as EbCalcField).DataFieldsUsed)
            //{
            //    string TName = calcfd.Split('.')[0];
            //    string fName = calcfd.Split('.')[1];
            //    globals[TName].Add(fName, new NTV { Name = fName, Type = (DbType)(field as EbCalcField).DbType, Value = Report.GetFieldtData(fName, 0) });
            //}



            //try
            //{
            //    globals.T1.x = new NTV { Name = "x", Type = DbType.Int32, Value = 10 };
            //    globals.T1.y = new NTV { Name = "y", Type = DbType.Int32, Value = 20 };           
            //    DateTime ts_start; DateTime ts_end; TimeSpan ts;

            //    // Console.WriteLine(CSharpScript.EvaluateAsync("return Environment.GetEnvironmentVariable(\"EB_REDIS_PASSWORD\");", ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core", "MSCorLib").WithImports("System.Dynamic", "System")).Result);

            //    ts_start = DateTime.Now;
            //    var script = CSharpScript.Create<int>("(T1.x * T1.y) + 2", ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic"), globalsType: typeof(Globals));
            //    script.Compile();
            //    ts_end = DateTime.Now;
            //    Console.WriteLine("ts-Compile1 : " + ts);
            //    ts = ts_end - ts_start;

            //    ts_start = DateTime.Now;
            //    Console.WriteLine((script.RunAsync(globals)).Result.ReturnValue);
            //    ts_end = DateTime.Now;
            //    ts = ts_end - ts_start;
            //    Console.WriteLine("ts-Compile2 : " + ts);

            //    ts_start = DateTime.Now;
            //    Console.WriteLine((script.RunAsync(globals)).Result.ReturnValue);
            //    ts_end = DateTime.Now;
            //    ts = ts_end - ts_start;
            //    Console.WriteLine("ts-Compile3 : " + ts);
            //}
            //catch (Exception e)
            //{
            //}          


            //iTextSharp.text.Font link = FontFactory.GetFont("Arial", 12, iTextSharp.text.Font.UNDERLINE, BaseColor.DarkGray);
            // Anchor anchor = new Anchor("xyz",link);
            //anchor.Reference = "http://eb_roby_dev.localhost:5000/ReportRender?refid=eb_roby_dev-eb_roby_dev-3-1127-1854?tab=" + JsonConvert.SerializeObject(Report.DataRow[Report.SerialNumber - 1]);
            // d.Add(anchor);            
            Report.Doc.NewPage();
            Report.DrawReportHeader();
            Report.DrawDetail();
            Report.Doc.Close();
            Report.Ms1.Position = 0;//important
            //if (Report.DataSourceRefId != string.Empty)
            //{
            //    Report.DataSet.Tables.Clear();
            //    Report.DataSet.Dispose();
            //}
            return new ReportRenderResponse { StreamWrapper = new MemorystreamWrapper(Report.Ms1) };

        }

        private void FillScriptCollection(List<EbReportField> fields)
        {
            foreach (EbReportField field in fields)
            {
                if (field is EbCalcField && !Report.ScriptCollection.ContainsKey(field.Name))
                {
                    Script script = CSharpScript.Create<dynamic>((field as EbCalcField).Expression, ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic"), globalsType: typeof(Globals));
                    script.Compile();
                    Report.ScriptCollection.Add(field.Name, script);
                }
            }
        }

        private void GetWatermarkImages()
        {
            var myFileService = base.ResolveService<FileService>();
            byte[] fileByte = null;
            if (Report.ReportObjects != null)
            {
                foreach (var field in Report.ReportObjects)
                {
                    if ((field as EbWaterMark).Image != string.Empty)
                    {
                        fileByte = myFileService.Post
                      (new DownloadFileRequest
                      {
                          FileDetails = new FileMeta
                          {
                              FileName = (field as EbWaterMark).Image + ".jpg",
                              FileType = "jpg"
                          }
                      });
                    }
                    Report.watermarkImages.Add((field as EbWaterMark).Image, fileByte);
                }
            }
        }
    }

    public partial class HeaderFooter : PdfPageEventHelper
    {
        private EbReport Report { get; set; }
        public override void OnStartPage(PdfWriter writer, Document document)
        {
        }
        public override void OnEndPage(PdfWriter writer, Document d)
        {
            Report.DrawPageHeader();
            Report.DrawPageFooter();
            if (Report.IsLastpage == true) Report.DrawReportFooter();
            Report.DrawWaterMark(Report.PdfReader, d, writer);
        }

        public HeaderFooter(EbReport _c) : base()
        {
            this.Report = _c;
        }
    }
}
