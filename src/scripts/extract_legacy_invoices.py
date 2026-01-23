import pytesseract
from PIL import Image
import cv2 
import numpy as np
import pandas as pd
import re
import os
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

class InvoiceOCRProcessor:
    """Enhanced OCR processor with precise invoice structure parsing"""
    
    def __init__(self, invoices_directory='Data/legacy_invoices', debug=False):
        """
        Initialize the OCR processor
        
        Args:
            invoices_directory (str): Path to directory containing invoice images
            debug (bool): If True, show detailed OCR output
        """
        self.invoices_directory = invoices_directory
        self.extracted_data = []
        self.debug = debug
        
        # Configure Tesseract path (Windows users)
        pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
    
    def preprocess_method_1(self, img):
        """Standard preprocessing"""
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        blurred = cv2.GaussianBlur(gray, (5, 5), 0)
        thresh = cv2.adaptiveThreshold(blurred, 255, cv2.ADAPTIVE_THRESH_GAUSSIAN_C, 
                                      cv2.THRESH_BINARY, 11, 2)
        return thresh
    
    def preprocess_method_2(self, img):
        """Otsu thresholding"""
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        _, thresh = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        return thresh
    
    def preprocess_method_3(self, img):
        """Enhanced contrast"""
        gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
        clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
        enhanced = clahe.apply(gray)
        _, thresh = cv2.threshold(enhanced, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
        return thresh
    
    def extract_text_from_image(self, image_path):
        """
        Extract text using multiple preprocessing methods
        
        Args:
            image_path (str): Path to the invoice image
            
        Returns:
            str: Best extracted text
        """
        try:
            img = cv2.imread(image_path)
            if img is None:
                logging.error(f"Could not read image: {image_path}")
                return ""
            
            results = []
            methods = [
                ("Adaptive", self.preprocess_method_1),
                ("Otsu", self.preprocess_method_2),
                ("CLAHE", self.preprocess_method_3),
            ]
            
            for method_name, preprocess_func in methods:
                try:
                    processed = preprocess_func(img)
                    pil_img = Image.fromarray(processed)
                    text = pytesseract.image_to_string(pil_img, lang='fra+eng', config='--psm 6')
                    
                    if text and text.strip():
                        results.append((method_name, text, len(text)))
                except Exception as e:
                    if self.debug:
                        logging.debug(f"{method_name} failed: {e}")
            
            if results:
                best_method, best_text, _ = max(results, key=lambda x: x[2])
                if self.debug:
                    logging.debug(f"Best method: {best_method}")
                return best_text
            return ""
                
        except Exception as e:
            logging.error(f"Error extracting text from {image_path}: {e}")
            return ""
    
    def parse_invoice_data(self, text, filename):
        """
        Parse invoice data with precise structure understanding
        
        Args:
            text (str): Raw extracted text from OCR
            filename (str): Name of the invoice file for reference
            
        Returns:
            dict: Parsed invoice data
        """
        try:
            if self.debug:
                logging.debug(f"\n{'='*70}")
                logging.debug(f"RAW OCR TEXT for {filename}:")
                logging.debug(f"{'='*70}")
                for i, line in enumerate(text.split('\n')[:30], 1):
                    if line.strip():
                        logging.debug(f"{i:2d}: {line}")
                logging.debug(f"{'='*70}\n")
            
            # Split into lines
            lines = text.split('\n')
            lines = [line.strip() for line in lines if line.strip()]
            
            invoice_data = {
                'Source_File': filename,
                'Date': None,
                'Order_Reference': None,
                'Customer_ID': None,
                'Customer_Name': None,
                'Product_Name': None,
                'Quantity': None,
                'Unit_Price': None,
                'Total_Revenue': None
            }
            
            # Process line by line with structure awareness
            for i, line in enumerate(lines):
                
                # Extract Date
                if not invoice_data['Date']:
                    if 'date' in line.lower() or re.search(r'\d{4}-\d{2}-\d{2}', line):
                        date_match = re.search(r'(\d{4}-\d{2}-\d{2})', line)
                        if date_match:
                            invoice_data['Date'] = date_match.group(1)
                
                # Extract Order Reference
                if not invoice_data['Order_Reference']:
                    if 'ref' in line.lower() or re.search(r'ORD-\d+', line, re.IGNORECASE):
                        ref_match = re.search(r'(ORD-\d+)', line, re.IGNORECASE)
                        if ref_match:
                            invoice_data['Order_Reference'] = ref_match.group(1)
                
                # Extract Customer ID - look for line with "Client ID: C####"
                if not invoice_data['Customer_ID']:
                    if re.search(r'client\s+id', line.lower()):
                        # ID is on this line
                        id_match = re.search(r'(C\d{4})', line, re.IGNORECASE)
                        if id_match:
                            invoice_data['Customer_ID'] = id_match.group(1)
                            
                            # Customer Name is on the NEXT line after "Nom:"
                            # Look ahead for the name
                            if i + 1 < len(lines):
                                next_line = lines[i + 1]
                                # Check if this line has "Nom:" pattern
                                if 'nom' in next_line.lower():
                                    # Extract name after "Nom:"
                                    name_match = re.search(r'nom[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)', next_line, re.IGNORECASE)
                                    if name_match:
                                        invoice_data['Customer_Name'] = name_match.group(1).strip()
                
                # Alternative: Look for "Nom: [Name]" pattern anywhere
                if not invoice_data['Customer_Name']:
                    name_match = re.search(r'nom[:\s]+([A-Z][a-z]+(?:\s+[A-Z][a-z]+)+)', line, re.IGNORECASE)
                    if name_match:
                        name = name_match.group(1).strip()
                        # Exclude if it contains table keywords or product names
                        if not re.search(r'produit|qte|prix|total|unit|HP|Dell|MacBook|Samsung|iPhone', name, re.IGNORECASE):
                            invoice_data['Customer_Name'] = name
                
                # Extract table data - look for product line
                # Pattern: Product name followed by numbers
                if not invoice_data['Product_Name']:
                    # Try to match table row: [Product] [Qty] [Price] [Total]
                    table_match = re.match(r'^([A-Za-z0-9\s]+?)\s+(\d{1})\s+(\d{5,7})\s+(\d{5,7})\s*$', line)
                    if table_match:
                        product = table_match.group(1).strip()
                        # Exclude if it's just "Produit" or contains "Qte"
                        if not re.search(r'^produit$|qte|prix|total', product, re.IGNORECASE):
                            invoice_data['Product_Name'] = product
                            invoice_data['Quantity'] = int(table_match.group(2))
                            invoice_data['Unit_Price'] = float(table_match.group(3))
                            invoice_data['Total_Revenue'] = float(table_match.group(4))
            
            # Fallback: Known product patterns
            if not invoice_data['Product_Name']:
                product_patterns = [
                    (r'HP\s+Vi[ec]tus\s+(\d+)', 'HP Victus {}'),
                    (r'MacBook\s+Air\s+(M\d+)', 'MacBook Air {}'),
                    (r'Samsung\s+S(\d+)\s+Ultra', 'Samsung S{} Ultra'),
                    (r'iPhone\s+(\d+)\s+Pro', 'iPhone {} Pro'),
                    (r'Dell\s+[xX]PS\s+(\d+)', 'Dell XPS {}'),
                ]
                for pattern, template in product_patterns:
                    match = re.search(pattern, text, re.IGNORECASE)
                    if match:
                        invoice_data['Product_Name'] = template.format(match.group(1))
                        break
            
            # Fallback: Extract Customer ID from standalone pattern
            if not invoice_data['Customer_ID']:
                all_ids = re.findall(r'\b(C1\d{3}|C10\d{2})\b', text)
                if all_ids:
                    invoice_data['Customer_ID'] = all_ids[0]
            
            # Fallback: Extract Customer Name - find 2-word capitalized names
            if not invoice_data['Customer_Name']:
                for line in lines:
                    # Look for exactly 2 capitalized words (typical name pattern)
                    name_match = re.match(r'^([A-Z][a-z]+\s+[A-Z][a-z]+)$', line)
                    if name_match:
                        name = name_match.group(1)
                        # Exclude product brand names
                        if not re.search(r'HP|Dell|Air|Pro|Ultra|Victus|MacBook|Samsung|iPhone|Client|Nom', name):
                            invoice_data['Customer_Name'] = name
                            break
            
            # Fallback: Extract numbers for missing fields
            if not invoice_data['Quantity'] or not invoice_data['Total_Revenue']:
                all_numbers = re.findall(r'\b(\d+)\b', text)
                numbers = [int(n) for n in all_numbers]
                
                # Small numbers (1-5) are likely quantities
                small = [n for n in numbers if 1 <= n <= 5]
                # Large numbers (100k+) are prices
                large = [n for n in numbers if 100000 <= n <= 9999999]
                
                if small and not invoice_data['Quantity']:
                    invoice_data['Quantity'] = small[0]
                
                if len(large) >= 2:
                    if not invoice_data['Unit_Price']:
                        invoice_data['Unit_Price'] = float(large[0])
                    if not invoice_data['Total_Revenue']:
                        invoice_data['Total_Revenue'] = float(large[1])
                elif len(large) == 1 and not invoice_data['Total_Revenue']:
                    invoice_data['Total_Revenue'] = float(large[0])
            
            # Calculate missing values
            if invoice_data['Quantity'] and invoice_data['Unit_Price'] and not invoice_data['Total_Revenue']:
                invoice_data['Total_Revenue'] = invoice_data['Quantity'] * invoice_data['Unit_Price']
            
            if invoice_data['Quantity'] and invoice_data['Total_Revenue'] and not invoice_data['Unit_Price']:
                invoice_data['Unit_Price'] = invoice_data['Total_Revenue'] / invoice_data['Quantity']
            
            # Validate and fix quantity
            if invoice_data['Quantity'] and invoice_data['Unit_Price'] and invoice_data['Total_Revenue']:
                expected_total = invoice_data['Quantity'] * invoice_data['Unit_Price']
                # If calculation is off, recalculate quantity
                if abs(expected_total - invoice_data['Total_Revenue']) > 1000:
                    correct_qty = round(invoice_data['Total_Revenue'] / invoice_data['Unit_Price'])
                    if 1 <= correct_qty <= 10:
                        invoice_data['Quantity'] = correct_qty
            
            # Log summary
            logging.info(f"Extracted from {filename}:")
            for key, value in invoice_data.items():
                if key != 'Source_File':
                    status = "✓" if value else "✗"
                    logging.info(f"  {status} {key}: {value}")
            
            return invoice_data
                
        except Exception as e:
            logging.error(f"Error parsing {filename}: {e}")
            import traceback
            traceback.print_exc()
            return None
    
    def process_all_invoices(self):
        """Process all invoice images"""
        logging.info("="*70)
        logging.info("STARTING OCR INVOICE PROCESSING")
        logging.info("="*70)
        
        if not os.path.exists(self.invoices_directory):
            logging.error(f"Directory not found: {self.invoices_directory}")
            return pd.DataFrame()
        
        image_files = [f for f in os.listdir(self.invoices_directory) 
                      if f.lower().endswith(('.jpg', '.jpeg', '.png', '.tif', '.tiff'))]
        
        if not image_files:
            logging.warning(f"No image files found in {self.invoices_directory}")
            return pd.DataFrame()
        
        image_files.sort()
        logging.info(f"Found {len(image_files)} invoice images to process\n")
        
        for idx, filename in enumerate(image_files, 1):
            image_path = os.path.join(self.invoices_directory, filename)
            
            logging.info(f"\n{'='*70}")
            logging.info(f"PROCESSING [{idx}/{len(image_files)}]: {filename}")
            logging.info(f"{'='*70}")
            
            extracted_text = self.extract_text_from_image(image_path)
            
            if not extracted_text:
                logging.warning(f"No text extracted from {filename}")
                continue
            
            invoice_data = self.parse_invoice_data(extracted_text, filename)
            
            if invoice_data:
                self.extracted_data.append(invoice_data)
            else:
                logging.warning(f"Failed to parse {filename}")
        
        df = pd.DataFrame(self.extracted_data)
        
        logging.info("\n" + "="*70)
        logging.info(f"OCR PROCESSING COMPLETED")
        logging.info(f"Successfully processed: {len(df)}/{len(image_files)} invoices")
        logging.info("="*70)
        
        return df
    
    def save_to_csv(self, df, output_file='Data/extracted/legacy_sales.csv'):
        """Save extracted data to CSV"""
        if df.empty:
            logging.warning("No data to save")
            return
        
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        df.to_csv(output_file, index=False, encoding='utf-8')
        
        logging.info("\n" + "="*70)
        logging.info("DATA SUMMARY")
        logging.info("="*70)
        logging.info(f"Total records: {len(df)}")
        
        if 'Total_Revenue' in df.columns:
            total_revenue = df['Total_Revenue'].sum()
            logging.info(f"Total revenue: {total_revenue:,.0f} DZD")
        
        logging.info("\nData Completeness:")
        completeness = df.notna().sum()
        for col in df.columns:
            if col != 'Source_File':
                count = completeness[col]
                pct = (count / len(df)) * 100
                status = "✓" if pct >= 80 else "⚠"
                logging.info(f"  {status} {col}: {count}/{len(df)} ({pct:.0f}%)")
        
        logging.info(f"\n✓ Data saved to: {output_file}")
        logging.info("="*70)
        
        print("\n" + "="*70)
        print("EXTRACTED DATA PREVIEW")
        print("="*70)
        print(df.to_string(index=False))
        print("="*70 + "\n")
    
    def process_and_save(self):
        """Complete workflow"""
        df = self.process_all_invoices()
        
        if not df.empty:
            self.save_to_csv(df)
        else:
            logging.error("No data was extracted!")
        
        return df


def main():
    """Main execution"""
    print("\n" + "="*70)
    print("TECHSTORE LEGACY INVOICE OCR EXTRACTION")
    print("="*70 + "\n")
    
    # Set debug=True to see raw OCR output
    debug_mode = True  # Enable to see what OCR actually reads
    
    try:
        processor = InvoiceOCRProcessor('Data/legacy_invoices', debug=debug_mode)
        df = processor.process_and_save()
        
        if df is not None and not df.empty:
            # Check completeness
            completeness = df.notna().mean() * 100
            if completeness['Customer_Name'] >= 80 and completeness['Product_Name'] >= 80:
                print("\n✓✓✓ EXTRACTION COMPLETED SUCCESSFULLY ✓✓✓\n")
            else:
                print("\n⚠ EXTRACTION COMPLETED WITH MISSING DATA ⚠\n")
                print("Tip: Check the debug output above to see raw OCR text")
                print("Some fields may be missing due to image quality")
        else:
            print("\n✗✗✗ EXTRACTION FAILED ✗✗✗\n")
            
    except Exception as e:
        logging.error(f"Critical error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()