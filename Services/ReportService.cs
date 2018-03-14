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
using System.Text;
using ExpressBase.ServiceStack.Services;
using ExpressBase.Security;
using System.DrawingCore.Text;
using System.DrawingCore;

namespace ExpressBase.ServiceStack
{
    public class ReportService : EbBaseService
    {
        private DataSourceColumnsResponse cresp = null;
        private DataSourceDataResponse dresp = null;

        //private iTextSharp.text.Font f = FontFactory.GetFont(FontFactory.HELVETICA, 12);
        public ReportService(IEbConnectionFactory _dbf) : base(_dbf) { }


        public ReportRenderResponse Get(ReportRenderRequest request)
        {
            //int count = iTextSharp.text.FontFactory.RegisterDirectory("E:\\ExpressBase.Core\\ExpressBase.Objects\\Fonts\\");
            //using (InstalledFontCollection col = new InstalledFontCollection())
            //{
            //    foreach (FontFamily fa in col.Families)
            //    {
            //        Console.WriteLine(fa.Name);
            //    }
            //}

            EbReport Report = null;
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
            Report.ValueScriptCollection = new Dictionary<string, Script>();
            Report.AppearanceScriptCollection = new Dictionary<string, Script>();
            Report.CurrentTimestamp = DateTime.Now;
            Report.UserName = request.Fullname;
            //-- END REPORT object INIT

            var myDataSourceservice = base.ResolveService<DataSourceService>();
            if (Report.DataSourceRefId != string.Empty)
            {
                Console.WriteLine("Report.DataSourceRefId   :" + Report.DataSourceRefId);
                //    dresp = myDataSourceservice.Any(new DataSourceDataRequest444 { RefId = Report.DataSourceRefId, Draw = 1, Start = 0, Length = 100 });
                //    Report.DataSet = dresp.DataSet;

                cresp = this.Redis.Get<DataSourceColumnsResponse>(string.Format("{0}_columns", Report.DataSourceRefId));
                if (cresp == null)
                    cresp = myDataSourceservice.Any(new DataSourceColumnsRequest
                    {
                        RefId = Report.DataSourceRefId
                    });
                Report.DataColumns = (cresp.Columns.Count > 1) ? cresp.Columns[1] : cresp.Columns[0];
                dresp = myDataSourceservice.Any(new DataSourceDataRequest { RefId = Report.DataSourceRefId, Draw = 1, Start = 0, Length = 100 });
                Report.DataRow = dresp.Data; Console.WriteLine("Rows: " + dresp.Data.Count);

            }

            iTextSharp.text.Rectangle rec = new iTextSharp.text.Rectangle(Report.WidthPt, Report.HeightPt);
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

            Report.GetWatermarkImages();

            foreach (EbReportHeader r_header in Report.ReportHeaders)
                this.FillScriptCollection(Report, r_header.Fields);

            foreach (EbReportFooter r_footer in Report.ReportFooters)
                this.FillScriptCollection(Report, r_footer.Fields);

            foreach (EbPageHeader p_header in Report.PageHeaders)
                this.FillScriptCollection(Report, p_header.Fields);

            foreach (EbReportDetail detail in Report.Detail)
                this.FillScriptCollection(Report, detail.Fields);

            foreach (EbPageFooter p_footer in Report.PageFooters)
                this.FillScriptCollection(Report, p_footer.Fields);


            //iTextSharp.text.Font link = FontFactory.GetFont("Arial", 12, iTextSharp.text.Font.UNDERLINE, BaseColor.DarkGray);
            // Anchor anchor = new Anchor("xyz",link);
            //anchor.Reference = "http://eb_roby_dev.localhost:5000/ReportRender?refid=eb_roby_dev-eb_roby_dev-3-1127-1854?tab=" + JsonConvert.SerializeObject(Report.DataRow[Report.SerialNumber - 1]);
            // d.Add(anchor);            
            Report.Doc.NewPage();
            Report.DrawReportHeader();
            Report.DrawDetail();
            Report.Doc.Close();
            if (Report.UserPassword != string.Empty || Report.OwnerPassword != string.Empty)
                Report.SetPassword();
            Report.Ms1.Position = 0;//important
            //if (Report.DataSourceRefId != string.Empty)
            //{
            //    Report.DataSet.Tables.Clear();
            //    Report.DataSet.Dispose();
            //}
            return new ReportRenderResponse { StreamWrapper = new MemorystreamWrapper(Report.Ms1) };

        }

        private void FillScriptCollection(EbReport Report, List<EbReportField> fields)
        {
            foreach (EbReportField field in fields)
            {
                try
                {
                    if (field is EbCalcField && !Report.ValueScriptCollection.ContainsKey(field.Name))
                    {

                        byte[] dataval = Convert.FromBase64String((field as EbCalcField).ValueExpression);
                        string decodedvalE = Encoding.UTF8.GetString(dataval);
                        Script valscript = CSharpScript.Create<dynamic>(decodedvalE, ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic"), globalsType: typeof(Globals));
                        valscript.Compile();
                        Report.ValueScriptCollection.Add(field.Name, valscript);

                    }
                    else if ((field is EbDataField && !Report.AppearanceScriptCollection.ContainsKey(field.Name) && (field as EbDataField).AppearanceExpression != ""))
                    {
                        byte[] dataapp = Convert.FromBase64String((field as EbDataField).AppearanceExpression);
                        string decodedAppE = Encoding.UTF8.GetString(dataapp);
                        Script appearscript = CSharpScript.Create<dynamic>(decodedAppE, ScriptOptions.Default.WithReferences("Microsoft.CSharp", "System.Core").WithImports("System.Dynamic"), globalsType: typeof(Globals));
                        appearscript.Compile();
                        Report.AppearanceScriptCollection.Add(field.Name, appearscript);
                    }
                }
                catch (Exception e)
                {

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
            Report.DrawWaterMark(d, writer);
        }

        public HeaderFooter(EbReport _c) : base()
        {
            this.Report = _c;
        }
    }
}
