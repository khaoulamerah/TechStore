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
    """Enhanced OCR processor for extracting data from scanned invoices"""
    
    def __init__(self, invoices_directory='data/legacy_invoices'):
        """
        Initialize the OCR processor
        
        Args:
            invoices_directory (str): Path to directory containing invoice images
        """
        self.invoices_directory = invoices_directory
        self.extracted_data = []
        
        # Configure Tesseract path if needed (Windows users)
        # Uncomment and adjust path if on Windows:
        # pytesseract.pytesseract.tesseract_cmd = r'C:\Program Files\Tesseract-OCR\tesseract.exe'
    
    def preprocess_image(self, image_path):
        """
        Enhanced image preprocessing for better OCR accuracy
        
        Args:
            image_path (str): Path to the invoice image
            
        Returns:
            numpy.ndarray: Preprocessed image ready for OCR
        """
        try:
            # Read image
            img = cv2.imread(image_path)
            
            if img is None:
                logging.error(f"Could not read image: {image_path}")
                return None
            
            # Convert to grayscale
            gray = cv2.cvtColor(img, cv2.COLOR_BGR2GRAY)
            
            # Increase contrast using CLAHE (Contrast Limited Adaptive Histogram Equalization)
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
            enhanced = clahe.apply(gray)
            
            # Denoise
            denoised = cv2.fastNlMeansDenoising(enhanced, None, 10, 7, 21)
            
            # Apply binary thresholding (Otsu's method works well for documents)
            _, thresh = cv2.threshold(denoised, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            
            # Optional: Dilation to make text bolder (helps with broken characters)
            kernel = np.ones((1, 1), np.uint8)
            processed = cv2.dilate(thresh, kernel, iterations=1)
            
            return processed
            
        except Exception as e:
            logging.error(f"Error preprocessing image {image_path}: {e}")
            return None
    
    def extract_text_from_image(self, image_path):
        """
        Extract raw text from invoice image using OCR with optimized settings
        
        Args:
            image_path (str): Path to the invoice image
            
        Returns:
            str: Extracted text from the image
        """
        try:
            # Preprocess the image
            processed_img = self.preprocess_image(image_path)
            
            if processed_img is None:
                return ""
            
            # Convert to PIL Image
            pil_img = Image.fromarray(processed_img)
            
            # Custom Tesseract configuration for better accuracy
            # PSM 6 = Assume a single uniform block of text
            # PSM 4 = Assume a single column of text of variable sizes
            custom_config = r'--oem 3 --psm 6'
            
            # Perform OCR with French + English
            text = pytesseract.image_to_string(
                pil_img, 
                lang='fra+eng',
                config=custom_config
            )
            
            return text
            
        except Exception as e:
            logging.error(f"Error extracting text from {image_path}: {e}")
            return ""
    
    def parse_invoice_data(self, text, filename):
        """
        Enhanced parsing with multiple pattern attempts and validation
        
        Args:
            text (str): Raw extracted text from OCR
            filename (str): Name of the invoice file for reference
            
        Returns:
            dict: Parsed invoice data or None if parsing fails
        """
        try:
            # Clean the text: normalize whitespace
            text = re.sub(r'\s+', ' ', text)
            
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
            
            # ===== Extract Date =====
            date_patterns = [
                r'Date\s*:?\s*(\d{4}-\d{2}-\d{2})',  # YYYY-MM-DD
                r'Date\s*:?\s*(\d{2}-\d{2}-\d{4})',  # DD-MM-YYYY
                r'Date\s*:?\s*(\d{2}/\d{2}/\d{4})',  # DD/MM/YYYY
                r'(\d{4}-\d{2}-\d{2})',               # Standalone date
            ]
            for pattern in date_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    invoice_data['Date'] = match.group(1)
                    break
            
            # ===== Extract Order Reference =====
            ref_patterns = [
                r'Ref\s*:?\s*(ORD-\d+)',
                r'Reference\s*:?\s*(ORD-\d+)',
                r'(ORD-\d+)',
            ]
            for pattern in ref_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    invoice_data['Order_Reference'] = match.group(1)
                    break
            
            # ===== Extract Customer ID =====
            customer_id_patterns = [
                r'Client\s+ID\s*:?\s*(C\d+)',
                r'Customer\s+ID\s*:?\s*(C\d+)',
                r'ID\s*:?\s*(C\d+)',
                r'\b(C\d{4})\b',  # Standalone customer ID like C1001
            ]
            for pattern in customer_id_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    invoice_data['Customer_ID'] = match.group(1)
                    break
            
            # ===== Extract Customer Name =====
            # Look for name after "Nom:" or after Customer ID
            name_patterns = [
                r'Nom\s*:?\s*([A-Z][a-z]+\s+[A-Z][a-z]+)',
                r'Name\s*:?\s*([A-Z][a-z]+\s+[A-Z][a-z]+)',
                r'C\d{4}\s+([A-Z][a-z]+\s+[A-Z][a-z]+)',  # After Customer ID
            ]
            for pattern in name_patterns:
                match = re.search(pattern, text)
                if match:
                    invoice_data['Customer_Name'] = match.group(1).strip()
                    break
            
            # ===== Extract Product Name =====
            # Look for known product patterns or text before quantity
            product_patterns = [
                r'Produit\s+([A-Za-z0-9\s]+?)\s+\d+\s+\d+',  # Produit [Name] Qty Price
                r'Product\s+([A-Za-z0-9\s]+?)\s+\d+\s+\d+',
                # Specific product names
                r'(HP\s+Victus\s+\d+)',
                r'(MacBook\s+Air\s+M\d+)',
                r'(Samsung\s+S\d+\s+Ultra)',
                r'(iPhone\s+\d+\s+Pro)',
                r'(Dell\s+XPS\s+\d+)',
                # Generic pattern: Capital letter followed by alphanumeric
                r'\n([A-Z][A-Za-z0-9\s]{3,30}?)\s+\d+\s+\d{5,}',
            ]
            for pattern in product_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    product_name = match.group(1).strip()
                    # Clean up the product name
                    product_name = re.sub(r'\s+', ' ', product_name)
                    invoice_data['Product_Name'] = product_name
                    break
            
            # ===== Extract Quantity =====
            # Look for "Qte" column or number patterns
            qty_patterns = [
                r'Qte\s+Prix\s+Unit\s+Total\s+[A-Za-z0-9\s]+?\s+(\d+)',  # In table
                r'Qte\s+(\d+)',
                r'Quantity\s*:?\s*(\d+)',
                # Pattern: Product Name followed by quantity (1-9)
                r'[A-Za-z0-9\s]+\s+(\d)\s+\d{5,}\s+\d{5,}',
            ]
            for pattern in qty_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    invoice_data['Quantity'] = int(match.group(1))
                    break
            
            # ===== Extract Unit Price =====
            unit_price_patterns = [
                r'Prix\s+Unit\.?\s+Total\s+[A-Za-z0-9\s]+?\s+\d+\s+(\d+)',
                r'Prix\s+Unit\.?\s*:?\s*(\d+)',
                r'Unit\s+Price\s*:?\s*(\d+)',
            ]
            for pattern in unit_price_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    invoice_data['Unit_Price'] = float(match.group(1))
                    break
            
            # ===== Extract Total Revenue =====
            total_patterns = [
                r'Total\s+[A-Za-z0-9\s]+?\s+\d+\s+\d+\s+(\d+)',  # In table row
                r'Total\s*:?\s*(\d+)',
                r'Montant\s*:?\s*(\d+)',
            ]
            for pattern in total_patterns:
                match = re.search(pattern, text, re.IGNORECASE)
                if match:
                    invoice_data['Total_Revenue'] = float(match.group(1))
                    break
            
            # ===== Validation & Calculation =====
            # If we have quantity and unit price but no total, calculate it
            if (invoice_data['Quantity'] and invoice_data['Unit_Price'] 
                and not invoice_data['Total_Revenue']):
                invoice_data['Total_Revenue'] = (
                    invoice_data['Quantity'] * invoice_data['Unit_Price']
                )
            
            # Log what was extracted for debugging
            logging.info(f"Extracted from {filename}:")
            for key, value in invoice_data.items():
                if value and key != 'Source_File':
                    logging.info(f"  {key}: {value}")
            
            # Check if we have minimum required data
            if invoice_data['Date'] and invoice_data['Total_Revenue']:
                return invoice_data
            else:
                missing = [k for k, v in invoice_data.items() if not v and k != 'Source_File']
                logging.warning(f"Incomplete extraction from {filename}. Missing: {missing}")
                return invoice_data
                
        except Exception as e:
            logging.error(f"Error parsing invoice data from {filename}: {e}")
            return None
    
    def process_all_invoices(self):
        """
        Process all invoice images in the directory
        
        Returns:
            pd.DataFrame: DataFrame containing all extracted invoice data
        """
        logging.info("="*70)
        logging.info("STARTING LEGACY INVOICE OCR PROCESSING")
        logging.info("="*70)
        
        if not os.path.exists(self.invoices_directory):
            logging.error(f"Directory not found: {self.invoices_directory}")
            return pd.DataFrame()
        
        # Get all image files
        image_files = [f for f in os.listdir(self.invoices_directory) 
                      if f.lower().endswith(('.jpg', '.jpeg', '.png', '.tif', '.tiff'))]
        
        if not image_files:
            logging.warning(f"No image files found in {self.invoices_directory}")
            return pd.DataFrame()
        
        # Sort files for consistent processing
        image_files.sort()
        
        logging.info(f"Found {len(image_files)} invoice images to process\n")
        
        # Process each invoice
        for idx, filename in enumerate(image_files, 1):
            image_path = os.path.join(self.invoices_directory, filename)
            
            logging.info(f"[{idx}/{len(image_files)}] Processing: {filename}")
            logging.info("-" * 70)
            
            # Extract text using OCR
            extracted_text = self.extract_text_from_image(image_path)
            
            if not extracted_text:
                logging.warning(f"No text extracted from {filename}\n")
                continue
            
            # Optional: Log extracted text for debugging
            # logging.debug(f"Raw OCR text:\n{extracted_text}\n")
            
            # Parse structured data from text
            invoice_data = self.parse_invoice_data(extracted_text, filename)
            
            if invoice_data:
                self.extracted_data.append(invoice_data)
                logging.info(f"✓ Successfully processed {filename}\n")
            else:
                logging.warning(f"✗ Failed to parse data from {filename}\n")
        
        # Create DataFrame
        df = pd.DataFrame(self.extracted_data)
        
        logging.info("="*70)
        logging.info(f"OCR PROCESSING COMPLETED")
        logging.info(f"Successfully processed: {len(df)}/{len(image_files)} invoices")
        logging.info("="*70)
        
        return df
    
    def save_to_csv(self, df, output_file='data/extracted/legacy_sales.csv'):
        """
        Save extracted invoice data to CSV
        
        Args:
            df (pd.DataFrame): DataFrame containing invoice data
            output_file (str): Path to output CSV file
        """
        if df.empty:
            logging.warning("No data to save")
            return
        
        # Create output directory if needed
        os.makedirs(os.path.dirname(output_file), exist_ok=True)
        
        # Display summary before saving
        logging.info("\nData Summary:")
        logging.info(f"Total records: {len(df)}")
        
        if 'Total_Revenue' in df.columns:
            total_revenue = df['Total_Revenue'].sum()
            logging.info(f"Total revenue from legacy sales: {total_revenue:,.0f} DZD")
        
        # Check data completeness
        completeness = df.notna().sum()
        logging.info("\nData Completeness:")
        for col in df.columns:
            if col != 'Source_File':
                pct = (completeness[col] / len(df)) * 100
                logging.info(f"  {col}: {completeness[col]}/{len(df)} ({pct:.1f}%)")
        
        # Save to CSV
        df.to_csv(output_file, index=False, encoding='utf-8')
        logging.info(f"\n✓ Data saved to: {output_file}")
    
    def process_and_save(self):
        """
        Complete workflow: process all invoices and save to file
        """
        df = self.process_all_invoices()
        
        if not df.empty:
            self.save_to_csv(df)
        
        return df


def create_manual_data():
    """
    Manual data entry as fallback or supplement to OCR
    Based on the visible invoice images
    """
    logging.info("\n" + "="*70)
    logging.info("USING MANUAL DATA ENTRY METHOD")
    logging.info("="*70)
    
    # Manually transcribed data from the 5 invoice images
    manual_data = [
        {
            'Source_File': 'invoice_001.jpg',
            'Date': '2022-09-22',
            'Order_Reference': 'ORD-5073',
            'Customer_ID': 'C1001',
            'Customer_Name': 'Sami Oukil',
            'Product_Name': 'HP Victus 15',
            'Quantity': 2,
            'Unit_Price': 125000.0,
            'Total_Revenue': 250000.0
        },
        {
            'Source_File': 'invoice_002.jpg',
            'Date': '2022-01-20',
            'Order_Reference': 'ORD-5421',
            'Customer_ID': 'C1003',
            'Customer_Name': 'Meriem Bouzid',
            'Product_Name': 'MacBook Air M2',
            'Quantity': 1,
            'Unit_Price': 195000.0,
            'Total_Revenue': 195000.0
        },
        {
            'Source_File': 'invoice_003.jpg',
            'Date': '2022-06-01',
            'Order_Reference': 'ORD-8447',
            'Customer_ID': 'C1004',
            'Customer_Name': 'Amine Rahmani',
            'Product_Name': 'Samsung S23 Ultra',
            'Quantity': 3,
            'Unit_Price': 185000.0,
            'Total_Revenue': 555000.0
        },
        {
            'Source_File': 'invoice_004.jpg',
            'Date': '2022-10-27',
            'Order_Reference': 'ORD-7940',
            'Customer_ID': 'C1025',
            'Customer_Name': 'Houda Mekki',
            'Product_Name': 'iPhone 14 Pro',
            'Quantity': 3,
            'Unit_Price': 230000.0,
            'Total_Revenue': 690000.0
        },
        {
            'Source_File': 'invoice_005.jpg',
            'Date': '2022-02-27',
            'Order_Reference': 'ORD-3931',
            'Customer_ID': 'C1002',
            'Customer_Name': 'Sofiane Khelifa',
            'Product_Name': 'Dell XPS 13',
            'Quantity': 2,
            'Unit_Price': 260000.0,
            'Total_Revenue': 520000.0
        }
    ]
    
    df = pd.DataFrame(manual_data)
    
    # Save to file
    os.makedirs('data/extracted', exist_ok=True)
    output_file = 'data/extracted/legacy_sales.csv'
    df.to_csv(output_file, index=False, encoding='utf-8')
    
    logging.info(f"\n✓ Manual data created: {len(df)} records")
    logging.info(f"✓ Total revenue: {df['Total_Revenue'].sum():,.0f} DZD")
    logging.info(f"✓ Saved to: {output_file}")
    
    return df


def main():
    """Main execution function"""
    
    print("\n" + "="*70)
    print("TECHSTORE LEGACY INVOICE EXTRACTION")
    print("="*70 + "\n")
    
    # Ask user for processing method
    print("Choose extraction method:")
    print("1. Automated OCR (requires Tesseract)")
    print("2. Manual data entry (pre-transcribed)")
    print("3. Try OCR first, fallback to manual if needed")
    
    choice = input("\nEnter choice (1/2/3) [default: 3]: ").strip() or "3"
    
    try:
        if choice == "1":
            # OCR only
            processor = InvoiceOCRProcessor('data/legacy_invoices')
            df = processor.process_and_save()
            
            if df is None or df.empty:
                logging.error("OCR failed to extract data")
            
        elif choice == "2":
            # Manual only
            df = create_manual_data()
            
        else:
            # Try OCR, fallback to manual
            try:
                processor = InvoiceOCRProcessor('data/legacy_invoices')
                df = processor.process_and_save()
                
                if df is None or df.empty or len(df) < 3:
                    logging.warning("\nOCR results incomplete, using manual data as backup")
                    df = create_manual_data()
                    
            except Exception as e:
                logging.error(f"OCR error: {e}")
                logging.info("Falling back to manual data entry")
                df = create_manual_data()
        
        print("\n" + "="*70)
        print("EXTRACTION COMPLETED SUCCESSFULLY")
        print("="*70)
        
        if df is not None and not df.empty:
            print(f"\n📊 Dataset Preview:\n")
            print(df.to_string(index=False))
        
    except Exception as e:
        logging.error(f"Critical error: {e}")
        import traceback
        traceback.print_exc()


if __name__ == "__main__":
    main()