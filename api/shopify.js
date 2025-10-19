export default async function handler(req, res) {
  // CORS headers - send on EVERY response
  res.setHeader('Access-Control-Allow-Origin', '*');
  res.setHeader('Access-Control-Allow-Methods', 'GET, POST, OPTIONS');
  res.setHeader('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.setHeader('Access-Control-Max-Age', '86400');
  
  // Handle preflight
  if (req.method === 'OPTIONS') {
    return res.status(204).end();
  }
  
  // Handle GET
  if (req.method === 'GET') {
    return res.status(200).json({ status: 'Backend is alive!' });
  }
  
  // Handle POST
  try {
    const { storeName, accessToken, action, upc } = req.body || {};
    
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
      return res.status(200).json({ success: true, shopName: data.shop.name });
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
    
    return res.status(200).json({ message: 'Backend ready' });
    
  } catch (error) {
    return res.status(500).json({ error: error.message });
  }
}
