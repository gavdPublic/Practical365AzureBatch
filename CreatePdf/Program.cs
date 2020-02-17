using iText.Kernel.Pdf;
using iText.Layout;
using iText.Layout.Element;
using Microsoft.WindowsAzure.Storage.Blob;
using System;
using System.Collections.Generic;
using System.IO;
using System.Xml;

namespace Practical365AzureBatch.CreatePdf
{
    public class Program
    {
        static void Main(string[] args)
        {
            // First argument: XML file path using env. variables %AZ_BATCH_NODE_SHARED_DIR%\filename.xml
            string inputFile = args[0];

            // Second argument: SAS of output container (with WRITE access)
            string outputContainerSas = args[1];

            // Read the XML content
            string content = File.ReadAllText(inputFile);
            XmlDocument myXmlDoc = new XmlDocument();
            myXmlDoc.LoadXml(content);

            string outputFileName = Path.GetFileNameWithoutExtension(inputFile) + ".pdf";

            Dictionary<string, string> fieldValues = new Dictionary<string, string>();
            foreach (XmlNode oneNode in myXmlDoc.SelectNodes("/Item/*"))
            {
                fieldValues[oneNode.Name] = oneNode.InnerText;
            }

            byte[] bytePdfOutput = CreatePdf(fieldValues);

            // Upload the PDF to the container
            UploadFileToContainer(outputFileName, bytePdfOutput, outputContainerSas);
        }

        static byte[] CreatePdf(Dictionary<string, string> fieldValues)
        {
            var myMemStream = new MemoryStream();
            var myPdfWriter = new PdfWriter(myMemStream);
            var myPdfDoc = new PdfDocument(myPdfWriter);
            var myDocument = new Document(myPdfDoc);

            foreach (string oneKey in fieldValues.Keys)
            {
                myDocument.Add(new Paragraph(oneKey.Replace("_", " ") + " - " + fieldValues[oneKey]));
            }

            myDocument.Close();

            byte[] bytePdfOutput = myMemStream.ToArray();

            return bytePdfOutput;
        }

        private static void UploadFileToContainer(string xmlFilePath, byte[] pdfOutput, string containerSas)
        {
            string blobName = xmlFilePath;

            // Container reference using the URI in the SAS
            CloudBlobContainer container = new CloudBlobContainer(new Uri(containerSas));

            // Upload the file (as a new blob) to the container
            CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
            blob.UploadFromByteArray(pdfOutput, 0, pdfOutput.Length);
        }
    }
}
