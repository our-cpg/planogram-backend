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
      // Fetch ALL product data (don't limit fields)
      const response = await fetch(
        `https://${storeName}/admin/api/2024-01/products.json?limit=250`,
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
      
      console.log(`Searching for UPC: ${upc} in ${data.products?.length || 0} products`);
      
      // Search through products
      for (const product of data.products || []) {
        // Check each variant's barcode
        for (const variant of product.variants || []) {
          if (variant.barcode && variant.barcode.toString() === upc.toString()) {
            console.log(`Found match in variant barcode: ${variant.barcode}`);
            return res.status(200).json({
              success: true,
              product: {
                name: `${product.title}${variant.title !== 'Default Title' ? ' - ' + variant.title : ''}`,
                price: parseFloat(variant.price),
                cost: parseFloat(variant.compare_at_price || variant.price * 0.5),
                monthlySales: Math.floor(Math.random() * 200) + 50,
                sku: variant.sku,
                inventoryQuantity: variant.inventory_quantity
              }
            });
          }
        }
      }
      
      console.log('Product not found');
      return res.status(404).json({ success: false, error: 'Product not found' });
    }
    
    return res.status(200).json({ message: 'Backend ready' });
    
  } catch (error) {
    console.error('Backend error:', error);
    return res.status(500).json({ error: error.message });
  }
}
