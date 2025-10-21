import express from 'express';
import cors from 'cors';
import pg from 'pg';

const app = express();
const PORT = process.env.PORT || 10000;
const { Pool } = pg;

// Database connection
const pool = new Pool({
  connectionString: process.env.DATABASE_URL,
  ssl: {
    rejectUnauthorized: false
  }
});

// CORS middleware
app.use((req, res, next) => {
  res.header('Access-Control-Allow-Origin', '*');
  res.header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS, PUT, DELETE');
  res.header('Access-Control-Allow-Headers', 'Content-Type, Authorization');
  res.header('Access-Control-Max-Age', '86400');
  
  if (req.method === 'OPTIONS') {
    return res.status(200).end();
  }
  
  next();
});

app.use(express.json());

// Initialize database tables
async function initDatabase() {
  try {
    await pool.query(`
      CREATE TABLE IF NOT EXISTS products (
        variant_id BIGINT PRIMARY KEY,
        product_id BIGINT NOT NULL,
        title TEXT NOT NULL,
        variant_title TEXT,
        barcode TEXT,
        sku TEXT,
        price DECIMAL(10,2),
        compare_at_price DECIMAL(10,2),
        cost DECIMAL(10,2),
        inventory_quantity INTEGER,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
      );
    `);

    await pool.query(`
      CREATE INDEX IF NOT EXISTS idx_barcode ON products(barcode);
    `);

    await pool.query(`
      CREATE TABLE IF NOT EXISTS sales_data (
        variant_id BIGINT PRIMARY KEY,
        monthly_sales INTEGER DEFAULT 0,
        last_updated TIMESTAMP DEFAULT NOW()
      );
    `);

    console.log('✅ Database tables initialized');
  } catch (error) {
    console.error('❌ Database init error:', error);
  }
}

// Import products from Shopify (MEMORY EFFICIENT + TITLE FILTER + NO TRUNCATE!)
async function importProducts(storeName, accessToken) {
  console.log('🔄 Starting product import from Shopify...');
  console.log('📋 Filtering: Only products with proper title case (not ALL CAPS)');
  
  try {
    let hasNextPage = true;
    let pageInfo = null;
    let pageCount = 0;
    let totalInserted = 0;
    let totalSkipped = 0;

    // ✅ REMOVED TRUNCATE - Products will persist now!
    // The ON CONFLICT clause handles updates automatically

    // Helper function to check if title is properly formatted
    const isProperlyFormatted = (title) => {
      // Skip if title is all uppercase (like "WAVING BUTTER POMADE")
      if (title === title.toUpperCase()) {
        return false;
      }
      
      // Must have at least one lowercase letter (like "Waving Butter Pomade")
      if (!/[a-z]/.test(title)) {
        return false;
      }
      
      return true;
    };

    // Fetch and insert in batches (memory efficient!)
    while (hasNextPage) {
      const url = pageInfo 
        ? `https://${storeName}.myshopify.com/admin/api/2024-01/products.json?limit=250&page_info=${pageInfo}`
        : `https://${storeName}.myshopify.com/admin/api/2024-01/products.json?limit=250`;

      const response = await fetch(url, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error(`Shopify API error: ${response.status}`);
      }

      const data = await response.json();
      const products = data.products || [];
      
      pageCount++;
      console.log(`📦 Fetched page ${pageCount}: ${products.length} products`);

      // Filter and insert products immediately (don't store in memory)
      for (const product of products) {
        // Skip if title is not properly formatted
        if (!isProperlyFormatted(product.title)) {
          totalSkipped++;
          continue;
        }

        for (const variant of product.variants) {
          try {
            await pool.query(`
              INSERT INTO products (
                variant_id, product_id, title, variant_title, barcode, sku, 
                price, compare_at_price, cost, inventory_quantity, 
                created_at, updated_at
              ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
              ON CONFLICT (variant_id) 
              DO UPDATE SET
                title = EXCLUDED.title,
                variant_title = EXCLUDED.variant_title,
                barcode = EXCLUDED.barcode,
                sku = EXCLUDED.sku,
                price = EXCLUDED.price,
                compare_at_price = EXCLUDED.compare_at_price,
                cost = EXCLUDED.cost,
                inventory_quantity = EXCLUDED.inventory_quantity,
                updated_at = EXCLUDED.updated_at
            `, [
              variant.id,
              product.id,
              product.title,
              variant.title !== 'Default Title' ? variant.title : null,
              variant.barcode || null,
              variant.sku || null,
              parseFloat(variant.price) || 0,
              variant.compare_at_price ? parseFloat(variant.compare_at_price) : null,
              variant.inventory_management ? parseFloat(variant.price) * 0.6 : null,
              variant.inventory_quantity || 0,
              product.created_at,
              product.updated_at
            ]);
            
            totalInserted++;
          } catch (err) {
            console.error(`❌ Error inserting variant ${variant.id}:`, err.message);
          }
        }
      }

      console.log(`✅ Page ${pageCount}: ${totalInserted} inserted, ${totalSkipped} skipped (ALL CAPS)`);

      // Check for next page
      const linkHeader = response.headers.get('Link');
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const nextMatch = linkHeader.match(/<[^>]*page_info=([^>&]+)>;\s*rel="next"/);
        pageInfo = nextMatch ? nextMatch[1] : null;
        hasNextPage = !!pageInfo;
      } else {
        hasNextPage = false;
      }

      // Small delay to avoid rate limits
      await new Promise(resolve => setTimeout(resolve, 500));
    }

    console.log(`✅ Import complete! ${totalInserted} products inserted, ${totalSkipped} skipped (ALL CAPS)`);
    return { 
      success: true, 
      message: `Successfully imported ${totalInserted} products (${totalSkipped} skipped due to ALL CAPS titles)`,
      total: totalInserted,
      skipped: totalSkipped
    };

  } catch (error) {
    console.error('❌ Import failed:', error);
    throw error;
  }
}

// Import sales data from Shopify Orders API
async function importSalesData(storeName, accessToken) {
  console.log('📊 Starting sales data import from Shopify Orders...');
  
  try {
    const thirtyDaysAgo = new Date();
    thirtyDaysAgo.setDate(thirtyDaysAgo.getDate() - 30);
    
    let allOrders = [];
    let hasNextPage = true;
    let pageInfo = null;

    while (hasNextPage) {
      const url = pageInfo 
        ? `https://${storeName}.myshopify.com/admin/api/2024-01/orders.json?limit=250&status=any&created_at_min=${thirtyDaysAgo.toISOString()}&page_info=${pageInfo}`
        : `https://${storeName}.myshopify.com/admin/api/2024-01/orders.json?limit=250&status=any&created_at_min=${thirtyDaysAgo.toISOString()}`;

      const response = await fetch(url, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        throw new Error(`Shopify Orders API error: ${response.status}`);
      }

      const data = await response.json();
      allOrders = allOrders.concat(data.orders || []);

      const linkHeader = response.headers.get('Link');
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const nextMatch = linkHeader.match(/<[^>]*page_info=([^>&]+)>;\s*rel="next"/);
        pageInfo = nextMatch ? nextMatch[1] : null;
        hasNextPage = !!pageInfo;
      } else {
        hasNextPage = false;
      }
    }

    console.log(`📦 Fetched ${allOrders.length} orders from last 30 days`);

    const salesByVariant = {};
    
    allOrders.forEach(order => {
      order.line_items?.forEach(item => {
        const variantId = item.variant_id;
        if (variantId) {
          salesByVariant[variantId] = (salesByVariant[variantId] || 0) + item.quantity;
        }
      });
    });

    for (const [variantId, quantity] of Object.entries(salesByVariant)) {
      await pool.query(`
        INSERT INTO sales_data (variant_id, monthly_sales, last_updated)
        VALUES ($1, $2, NOW())
        ON CONFLICT (variant_id) 
        DO UPDATE SET 
          monthly_sales = $2,
          last_updated = NOW()
      `, [variantId, quantity]);
    }

    console.log(`✅ Sales data imported for ${Object.keys(salesByVariant).length} variants`);
    return { success: true, message: 'Sales data imported successfully' };

  } catch (error) {
    console.error('❌ Sales import failed:', error);
    throw error;
  }
}

// API Endpoints

// Health check
app.get('/', (req, res) => {
  res.json({ status: 'Planogram Backend Running', timestamp: new Date().toISOString() });
});

// Shopify operations endpoint
app.post('/api/shopify', async (req, res) => {
  const { storeName, accessToken, action } = req.body;

  if (!storeName || !accessToken) {
    return res.status(400).json({ error: 'Store name and access token required' });
  }

  try {
    if (action === 'connect') {
      const testUrl = `https://${storeName}.myshopify.com/admin/api/2024-01/shop.json`;
      const response = await fetch(testUrl, {
        headers: {
          'X-Shopify-Access-Token': accessToken,
          'Content-Type': 'application/json'
        }
      });

      if (!response.ok) {
        return res.status(401).json({ error: 'Invalid Shopify credentials' });
      }

      const data = await response.json();
      res.json({ 
        success: true, 
        shop: data.shop.name,
        message: 'Connected to Shopify successfully'
      });

    } else if (action === 'refreshCache') {
      console.log('🔄 Manual cache refresh requested');
      
      await importProducts(storeName, accessToken);
      await importSalesData(storeName, accessToken);
      
      const countResult = await pool.query('SELECT COUNT(*) as count FROM products');
      const productCount = parseInt(countResult.rows[0].count);
      
      res.json({ 
        success: true, 
        message: `Cache refreshed! ${productCount} products in database`,
        productCount
      });

    } else {
      res.status(400).json({ error: 'Invalid action' });
    }

  } catch (error) {
    console.error('❌ API Error:', error);
    res.status(500).json({ error: error.message });
  }
});

// UPC lookup endpoint
app.get('/api/product/:upc', async (req, res) => {
  const { upc } = req.params;
  
  console.log(`🔍 Searching database for UPC: "${upc}"`);

  try {
    const result = await pool.query(`
      SELECT 
        p.*,
        COALESCE(s.monthly_sales, 0) as monthly_sales
      FROM products p
      LEFT JOIN sales_data s ON p.variant_id = s.variant_id
      WHERE p.barcode = $1
      LIMIT 1
    `, [upc]);

    if (result.rows.length === 0) {
      console.log(`❌ No product found with barcode: "${upc}"`);
      return res.status(404).json({ error: 'Product not found' });
    }

    const product = result.rows[0];
    console.log(`✅ Found product: ${product.title}`);

    res.json({
      title: product.title,
      variantTitle: product.variant_title,
      price: parseFloat(product.price),
      cost: product.cost ? parseFloat(product.cost) : parseFloat(product.price) * 0.6,
      monthlySales: parseInt(product.monthly_sales) || 0,
      barcode: product.barcode,
      sku: product.sku,
      inventoryQuantity: product.inventory_quantity
    });

  } catch (error) {
    console.error('❌ Database error:', error);
    res.status(500).json({ error: 'Database query failed' });
  }
});

// Database stats endpoint
app.get('/api/stats', async (req, res) => {
  try {
    const productCount = await pool.query('SELECT COUNT(*) as count FROM products');
    const salesCount = await pool.query('SELECT COUNT(*) as count FROM sales_data');
    
    res.json({
      products: parseInt(productCount.rows[0].count),
      salesTracked: parseInt(salesCount.rows[0].count)
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

// Initialize and start server
async function startServer() {
  await initDatabase();
  app.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
  });
}

// Test database connection
pool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error('❌ Database connection failed:', err);
  } else {
    console.log('📊 Database connected');
  }
});

startServer();
