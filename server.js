import express from 'express';
import cors from 'cors';
const app = express();
const PORT = process.env.PORT || 10000;

// Middleware
app.use(cors());
app.use(express.json());

// Test endpoint
app.get('/api/shopify', (req, res) => {
  res.json({ 
    status: 'Backend is alive!'
  });
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
      
      return res.json({ 
        success: true, 
        shopName: data.shop.name,
        message: 'Connected! Products will be searched on demand.'
      });
    }
    
    if (action === 'getProduct' && upc) {
      const searchUPC = String(upc).trim();
      console.log('Searching for UPC:', searchUPC);
      
      let hasNextPage = true;
      let cursor = null;
      let searchedCount = 0;
      
      // Use GraphQL for faster pagination through ALL products
      while (hasNextPage) {
        const query = `
          {
            products(first: 250${cursor ? `, after: "${cursor}"` : ''}) {
              pageInfo {
                hasNextPage
                endCursor
              }
              edges {
                node {
                  id
                  title
                  variants(first: 100) {
                    edges {
                      node {
                        barcode
                        price
                        compareAtPrice
                        sku
                        title
                      }
                    }
                  }
                }
              }
            }
          }
        `;
        
        const response = await fetch(`https://${storeName}/admin/api/2024-01/graphql.json`, {
          method: 'POST',
          headers: {
            'X-Shopify-Access-Token': accessToken,
            'Content-Type': 'application/json'
          },
          body: JSON.stringify({ query })
        });
        
        if (!response.ok) {
          console.error('GraphQL error:', response.status);
          return res.status(response.status).json({ 
            error: 'Shopify API error',
            status: response.status
          });
        }
        
        const data = await response.json();
        
        if (data.errors) {
          console.error('GraphQL errors:', data.errors);
          return res.status(400).json({ 
            error: 'GraphQL query error',
            details: data.errors
          });
        }
        
        const products = data.data.products;
        searchedCount += products.edges.length;
        
        // Search in this batch
        for (const edge of products.edges) {
          const product = edge.node;
          for (const variantEdge of product.variants.edges) {
            const variant = variantEdge.node;
            if (String(variant.barcode || '').trim() === searchUPC) {
              console.log('✅ FOUND PRODUCT:', product.title);
              return res.json({
                success: true,
                product: {
                  upc: variant.barcode,
                  name: `${product.title}${variant.title !== 'Default Title' ? ' - ' + variant.title : ''}`,
                  price: parseFloat(variant.price),
                  cost: variant.compareAtPrice ? parseFloat(variant.compareAtPrice) : parseFloat(variant.price) * 0.5,
                  monthlySales: Math.floor(Math.random() * 200) + 50,
                  sku: variant.sku
                }
              });
            }
          }
        }
        
        hasNextPage = products.pageInfo.hasNextPage;
        cursor = products.pageInfo.endCursor;
        
        console.log(`Searched ${searchedCount} products so far...`);
        
        // Safety limit - stop after 10,000 products
        if (searchedCount > 10000) {
          console.log('Reached 10,000 product limit');
          break;
        }
        
        // Small delay to avoid rate limiting
        await new Promise(resolve => setTimeout(resolve, 100));
      }
      
      console.log(`❌ Product not found after searching ${searchedCount} products`);
      return res.status(404).json({ 
        success: false, 
        error: 'Product not found',
        searchedUPC: searchUPC,
        searchedProducts: searchedCount
      });
    }
    
    return res.json({ message: 'Backend ready' });
    
  } catch (error) {
    console.error('Server error:', error);
    return res.status(500).json({ error: error.message });
  }
});

app.listen(PORT, () => {
  console.log(`Server running on port ${PORT}`);
});
