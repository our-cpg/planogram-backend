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

    // UPDATED: Add columns for different time periods
    await pool.query(`
      CREATE TABLE IF NOT EXISTS sales_data (
        variant_id BIGINT PRIMARY KEY,
        daily_sales INTEGER DEFAULT 0,
        weekly_sales INTEGER DEFAULT 0,
        monthly_sales INTEGER DEFAULT 0,
        quarterly_sales INTEGER DEFAULT 0,
        yearly_sales INTEGER DEFAULT 0,
        all_time_sales INTEGER DEFAULT 0,
        last_updated TIMESTAMP DEFAULT NOW()
      );
    `);

    // Add new columns to existing table if they don't exist
    await pool.query(`
      ALTER TABLE sales_data 
      ADD COLUMN IF NOT EXISTS daily_sales INTEGER DEFAULT 0,
      ADD COLUMN IF NOT EXISTS weekly_sales INTEGER DEFAULT 0,
      ADD COLUMN IF NOT EXISTS quarterly_sales INTEGER DEFAULT 0,
      ADD COLUMN IF NOT EXISTS yearly_sales INTEGER DEFAULT 0,
      ADD COLUMN IF NOT EXISTS all_time_sales INTEGER DEFAULT 0;
    `);

    console.log('✅ Database tables initialized');
  } catch (error) {
    console.error('❌ Database init error:', error);
  }
}

// Import products from Shopify (MEMORY EFFICIENT + TITLE FILTER)
async function importProducts(storeName, accessToken) {
  console.log('🔄 Starting product import from Shopify...');
  console.log('📋 Filtering: Only products with proper title case (not ALL CAPS)');
  
  try {
    let hasNextPage = true;
    let pageInfo = null;
    let pageCount = 0;
    let totalInserted = 0;
    let totalSkipped = 0;

    const isProperlyFormatted = (title) => {
      if (title === title.toUpperCase()) return false;
      if (!/[a-z]/.test(title)) return false;
      return true;
    };

    const cleanStoreName = (name) => {
      return name.replace('.myshopify.com', '');
    };

    const storeNameClean = cleanStoreName(storeName);

    while (hasNextPage) {
      const url = pageInfo 
        ? `https://${storeNameClean}.myshopify.com/admin/api/2024-01/products.json?limit=250&page_info=${pageInfo}`
        : `https://${storeNameClean}.myshopify.com/admin/api/2024-01/products.json?limit=250`;

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

      for (const product of products) {
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

      console.log(`✅ Page ${pageCount}: ${totalInserted} inserted, ${totalSkipped} skipped`);

      const linkHeader = response.headers.get('Link');
      if (linkHeader && linkHeader.includes('rel="next"')) {
        const nextMatch = linkHeader.match(/<[^>]*page_info=([^>&]+)>;\s*rel="next"/);
        pageInfo = nextMatch ? nextMatch[1] : null;
        hasNextPage = !!pageInfo;
      } else {
        hasNextPage = false;
      }

      await new Promise(resolve => setTimeout(resolve, 500));
    }

    console.log(`✅ Import complete! ${totalInserted} products inserted, ${totalSkipped} skipped`);
    return { 
      success: true, 
      message: `Successfully imported ${totalInserted} products (${totalSkipped} skipped)`,
      total: totalInserted,
      skipped: totalSkipped
    };

  } catch (error) {
    console.error('❌ Import failed:', error);
    throw error;
  }
}

// UPDATED: Import sales data for multiple time periods
async function importSalesData(storeName, accessToken) {
  console.log('📊 Starting sales data import from Shopify Orders...');
  
  try {
    const now = new Date();
    const oneDayAgo = new Date(now.getTime() - 24 * 60 * 60 * 1000);
    const oneWeekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    const oneMonthAgo = new Date(now.getTime() - 30 * 24 * 60 * 60 * 1000);
    const threeMonthsAgo = new Date(now.getTime() - 90 * 24 * 60 * 60 * 1000);
    const oneYearAgo = new Date(now.getTime() - 365 * 24 * 60 * 60 * 1000);
    
    const storeNameClean = storeName.replace('.myshopify.com', '');

    // Fetch all orders from the past year (to get all-time data)
    let allOrders = [];
    let hasNextPage = true;
    let pageInfo = null;
    let pageCount = 0;

    console.log('📥 Fetching orders from last year...');

    while (hasNextPage) {
      const url = pageInfo 
        ? `https://${storeNameClean}.myshopify.com/admin/api/2024-01/orders.json?limit=250&status=any&created_at_min=${oneYearAgo.toISOString()}&page_info=${pageInfo}`
        : `https://${storeNameClean}.myshopify.com/admin/api/2024-01/orders.json?limit=250&status=any&created_at_min=${oneYearAgo.toISOString()}`;

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
      pageCount++;
      console.log(`📦 Fetched page ${pageCount}: ${data.orders?.length || 0} orders`);

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

    console.log(`✅ Fetched ${allOrders.length} total orders from last year`);

    // Calculate sales for each time period
    const salesByVariant = {};
    
    allOrders.forEach(order => {
      const orderDate = new Date(order.created_at);
      
      order.line_items?.forEach(item => {
        const variantId = item.variant_id;
        if (!variantId) return;

        if (!salesByVariant[variantId]) {
          salesByVariant[variantId] = {
            daily: 0,
            weekly: 0,
            monthly: 0,
            quarterly: 0,
            yearly: 0,
            allTime: 0
          };
        }

        const qty = item.quantity;
        salesByVariant[variantId].allTime += qty;
        
        if (orderDate >= oneDayAgo) salesByVariant[variantId].daily += qty;
        if (orderDate >= oneWeekAgo) salesByVariant[variantId].weekly += qty;
        if (orderDate >= oneMonthAgo) salesByVariant[variantId].monthly += qty;
        if (orderDate >= threeMonthsAgo) salesByVariant[variantId].quarterly += qty;
        if (orderDate >= oneYearAgo) salesByVariant[variantId].yearly += qty;
      });
    });

    console.log(`📊 Processing sales data for ${Object.keys(salesByVariant).length} variants...`);

    // Insert/update sales data
    let updated = 0;
    for (const [variantId, sales] of Object.entries(salesByVariant)) {
      await pool.query(`
        INSERT INTO sales_data (
          variant_id, 
          daily_sales, 
          weekly_sales, 
          monthly_sales, 
          quarterly_sales, 
          yearly_sales, 
          all_time_sales,
          last_updated
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
        ON CONFLICT (variant_id) 
        DO UPDATE SET 
          daily_sales = $2,
          weekly_sales = $3,
          monthly_sales = $4,
          quarterly_sales = $5,
          yearly_sales = $6,
          all_time_sales = $7,
          last_updated = NOW()
      `, [
        variantId, 
        sales.daily, 
        sales.weekly, 
        sales.monthly, 
        sales.quarterly, 
        sales.yearly, 
        sales.allTime
      ]);
      updated++;
    }

    console.log(`✅ Sales data imported for ${updated} variants`);
    console.log(`📊 Breakdown:`);
    console.log(`   - Daily sales tracked`);
    console.log(`   - Weekly sales tracked`);
    console.log(`   - Monthly sales tracked`);
    console.log(`   - Quarterly sales tracked`);
    console.log(`   - Yearly sales tracked`);
    console.log(`   - All-time sales tracked`);
    
    return { 
      success: true, 
      message: 'Sales data imported for all time periods',
      variantsUpdated: updated
    };

  } catch (error) {
    console.error('❌ Sales import failed:', error);
    throw error;
  }
}

// API Endpoints

app.get('/', (req, res) => {
  res.json({ status: 'Planogram Backend Running', timestamp: new Date().toISOString() });
});

app.post('/api/shopify', async (req, res) => {
  const { storeName, accessToken, action } = req.body;

  if (!storeName || !accessToken) {
    return res.status(400).json({ error: 'Store name and access token required' });
  }

  try {
    if (action === 'connect') {
      const storeNameClean = storeName.replace('.myshopify.com', '');
      const testUrl = `https://${storeNameClean}.myshopify.com/admin/api/2024-01/shop.json`;
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

// UPDATED: Return sales data for all time periods
app.get('/api/product/:upc', async (req, res) => {
  const { upc } = req.params;
  
  console.log(`🔍 Searching database for UPC: "${upc}"`);

  try {
    const result = await pool.query(`
      SELECT 
        p.*,
        COALESCE(s.daily_sales, 0) as daily_sales,
        COALESCE(s.weekly_sales, 0) as weekly_sales,
        COALESCE(s.monthly_sales, 0) as monthly_sales,
        COALESCE(s.quarterly_sales, 0) as quarterly_sales,
        COALESCE(s.yearly_sales, 0) as yearly_sales,
        COALESCE(s.all_time_sales, 0) as all_time_sales
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
    console.log(`📊 Sales: Daily=${product.daily_sales}, Weekly=${product.weekly_sales}, Monthly=${product.monthly_sales}`);

    res.json({
      title: product.title,
      variantTitle: product.variant_title,
      price: parseFloat(product.price),
      cost: product.cost ? parseFloat(product.cost) : parseFloat(product.price) * 0.6,
      dailySales: parseInt(product.daily_sales) || 0,
      weeklySales: parseInt(product.weekly_sales) || 0,
      monthlySales: parseInt(product.monthly_sales) || 0,
      quarterlySales: parseInt(product.quarterly_sales) || 0,
      yearlySales: parseInt(product.yearly_sales) || 0,
      allTimeSales: parseInt(product.all_time_sales) || 0,
      barcode: product.barcode,
      sku: product.sku,
      inventoryQuantity: product.inventory_quantity
    });

  } catch (error) {
    console.error('❌ Database error:', error);
    res.status(500).json({ error: 'Database query failed' });
  }
});

app.get('/api/stats', async (req, res) => {
  try {
    const productCount = await pool.query('SELECT COUNT(*) as count FROM products');
    const salesCount = await pool.query('SELECT COUNT(*) as count FROM sales_data WHERE monthly_sales > 0');
    const totalSales = await pool.query('SELECT SUM(monthly_sales) as total FROM sales_data');
    
    res.json({
      products: parseInt(productCount.rows[0].count),
      productsWithSales: parseInt(salesCount.rows[0].count),
      totalMonthlySales: parseInt(totalSales.rows[0].total) || 0
    });
  } catch (error) {
    res.status(500).json({ error: error.message });
  }
});

async function startServer() {
  await initDatabase();
  app.listen(PORT, () => {
    console.log(`🚀 Server running on port ${PORT}`);
  });
}

pool.query('SELECT NOW()', (err, res) => {
  if (err) {
    console.error('❌ Database connection failed:', err);
  } else {
    console.log('📊 Database connected');
  }
});

startServer();
