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

    if (action === 'debugProduct') {
      const response = await fetch(
        `https://${storeName}/admin/api/2024-01/products.json?limit=250&title=Blasting Freeze Spray`,
        {
          headers: {
            'X-Shopify-Access-Token': accessToken,
            'Content-Type': 'application/json'
          }
        }
      );
      
      const data = await response.json();
      
      return res.json({
        found: data.products.length,
        products: data.products.map(p => ({
          title: p.title,
          id: p.id,
          variants: p.variants.map(v => ({
            title: v.title,
            barcode: v.barcode,
            barcodeType: typeof v.barcode,
            barcodeLength: v.barcode ? v.barcode.length : 0,
            sku: v.sku
          }))
        }))
      });
    }
    
    if (action === 'getProduct' && upc) {
      const searchUPC = String(upc).trim();
      let allProducts = [];
      let hasNextPage = true;
      let pageInfo = null;
      
      // Paginate through ALL products (no limit)
      while (hasNextPage) {
        const url = pageInfo 
          ? `https://${storeName}/admin/api/2024-01/products.json?limit=250&page_info=${pageInfo}`
          : `https://${storeName}/admin/api/2024-01/products.json?limit=250`;
          
        const response = await fetch(url, {
          headers: {
            'X-Shopify-Access-Token': accessToken,
            'Content-Type': 'application/json'
          }
        });
        
        if (!response.ok) {
          return res.status(response.status).json({ error: 'Failed to fetch products' });
        }
        
        const data = await response.json();
        allProducts = allProducts.concat(data.products);
        
        // Check for next page in Link header
        const linkHeader = response.headers.get('link');
        if (linkHeader && linkHeader.includes('rel="next"')) {
          const match = linkHeader.match(/page_info=([^&>]+)/);
          pageInfo = match ? match[1] : null;
        } else {
          hasNextPage = false;
        }
      }
      
      // Now search for the barcode
      for (const product of allProducts) {
        for (const variant of product.variants) {
          if (String(variant.barcode || '').trim() === searchUPC) {
            return res.json({
              success: true,
              product: {
                name: `${product.title}${variant.title !== 'Default Title' ? ' - ' + variant.title : ''}`,
                price: parseFloat(variant.price),
                cost: parseFloat(variant.compare_at_price || variant.price * 0.5),
                monthlySales: Math.floor(Math.random() * 200) + 50,
                sku: variant.sku
              }
            });
          }
        }
      }
      
      return res.status(404).json({ 
        error: 'Product not found',
        searchedProducts: allProducts.length,
        searchingFor: searchUPC
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
