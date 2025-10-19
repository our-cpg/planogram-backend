import express from 'express';
import cors from 'cors';
const app = express();
const PORT = process.env.PORT || 10000;

// Middleware
app.use(cors());
app.use(express.json());

// Test endpoint
app.get('/api/shopify', (req, res) => {
  res.json({ status: 'Backend is alive!' });
});

// Shopify endpoints
app.post('/api/shopify', async (req, res) => {
  const { storeName, accessToken, action, upc } = req.body || {};
  
  try {
    if (action === 'test') {
      const response = await fetch(`https://${storeName}/admin/api/2024-01/shop.json`, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });
      
      if (!response.ok) {
        return res.status(response.status).json({ error: 'Shopify API error' });
      }
      
      const data = await response.json();
      return res.json({ success: true, shopName: data.shop.name });
    }
    
    if (action === 'getProduct' && upc) {
      const searchUPC = String(upc).trim();
      
      const response = await fetch(
        `https://${storeName}/admin/api/2024-01/products.json?limit=10`,
        {
          headers: {
            'X-Shopify-Access-Token': accessToken,
            'Content-Type': 'application/json'
          }
        }
      );
      
      if (!response.ok) {
        return res.status(response.status).json({ error: 'Failed to fetch products' });
      }
      
      const data = await response.json();
      
      // Just return the first 10 products with their barcodes so we can see what Shopify gives us
      const productInfo = data.products.map(p => ({
        title: p.title,
        variants: p.variants.map(v => ({
          title: v.title,
          barcode: v.barcode,
          sku: v.sku,
          price: v.price
        }))
      }));
      
      return res.status(200).json({
        searchingFor: searchUPC,
        productsReturned: data.products.length,
        products: productInfo
      });
    }
    
    return res.json({ message: 'Backend ready' });
    
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
