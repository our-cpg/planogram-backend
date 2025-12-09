import express from 'express';
import cors from 'cors';
import pg from 'pg';
import crypto from 'crypto';

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

// Track order processing status
let orderProcessingStatus = {
  isProcessing: false,
  lastCompleted: null,
  lastResult: null,
  progress: { processed: 0, total: 0 }
};

// Initialize database tables
async function initDatabase() {
  console.log('🔄 Initializing database...');
  
  try {
    // Create products table
    console.log('📦 Creating products table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS products (
        variant_id TEXT PRIMARY KEY,
        product_id BIGINT NOT NULL,
        title TEXT NOT NULL,
        variant_title TEXT,
        barcode TEXT,
        sku TEXT,
        price DECIMAL(10,2),
        compare_at_price DECIMAL(10,2),
        cost DECIMAL(10,2),
        inventory_quantity INTEGER,
        vendor TEXT,
        tags TEXT,
        distributor TEXT,
        created_at TIMESTAMP,
        updated_at TIMESTAMP
      );
    `);
    console.log('✅ Products table ready');

    // Create sales_data table
    console.log('📊 Creating sales_data table...');
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
    console.log('✅ Sales data table ready');

    // Create orders table - optimized for Order Blitz
    console.log('📋 Creating orders table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS orders (
        order_id BIGINT PRIMARY KEY,
        order_number TEXT,
        customer_id BIGINT,
        customer_email_hash TEXT,
        total_price DECIMAL(10,2),
        subtotal_price DECIMAL(10,2),
        total_tax DECIMAL(10,2),
        order_date TIMESTAMPTZ,
        is_returning_customer BOOLEAN DEFAULT FALSE,
        created_at TIMESTAMPTZ DEFAULT NOW(),
        updated_at TIMESTAMPTZ DEFAULT NOW()
      );
    `);
    console.log('✅ Orders table ready');

    // Create index on order_date for fast queries
    try {
      await pool.query(`CREATE INDEX IF NOT EXISTS idx_order_date ON orders(order_date DESC);`);
      console.log('✅ Order date index created');
    } catch (err) {
      console.log('⚠️ Index already exists');
    }

    // Create order_items table
    console.log('📦 Creating order_items table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS order_items (
        id SERIAL PRIMARY KEY,
        order_id BIGINT REFERENCES orders(order_id) ON DELETE CASCADE,
        variant_id TEXT,
        product_id TEXT,
        title TEXT,
        variant_title TEXT,
        quantity INTEGER,
        price DECIMAL(10,2),
        cart_position INTEGER,
        customer_is_returning BOOLEAN DEFAULT FALSE,
        UNIQUE(order_id, variant_id, cart_position)
      );
    `);
    console.log('✅ Order items table ready');

    // Create product_correlations table
    console.log('🤝 Creating product_correlations table...');
    await pool.query(`
      CREATE TABLE IF NOT EXISTS product_correlations (
        id SERIAL PRIMARY KEY,
        variant_a_id TEXT NOT NULL,
        variant_b_id TEXT NOT NULL,
        co_purchase_count INTEGER DEFAULT 1,
        correlation_score DECIMAL(5,4) DEFAULT 0,
        last_updated TIMESTAMP DEFAULT NOW(),
        UNIQUE(variant_a_id, variant_b_id)
      );
    `);
    console.log('✅ Product correlations table ready');

    console.log('✅ Database initialization complete!');
  } catch (error) {
    console.error('❌ Database initialization error:', error);
    throw error;
  }
}

// 🔥 ORDER BLITZ - Optimized incremental sync
app.post('/api/order-blitz', async (req, res) => {
  const { storeName, accessToken } = req.body;

  if (!storeName || !accessToken) {
    return res.status(400).json({ error: 'Missing Shopify credentials' });
  }

  // Prevent concurrent processing
  if (orderProcessingStatus.isProcessing) {
    return res.json({
      success: true,
      processing: true,
      message: 'Order processing already in progress'
    });
  }

  try {
    orderProcessingStatus.isProcessing = true;
    orderProcessingStatus.progress = { processed: 0, total: 0 };
    console.log('🔥 Order Blitz started');

    // Get the most recent order date from database
    const lastOrderResult = await pool.query(`
      SELECT MAX(order_date) as last_order_date 
      FROM orders
    `);
    
    const lastOrderDate = lastOrderResult.rows[0]?.last_order_date;
    const sinceDate = lastOrderDate 
      ? new Date(lastOrderDate).toISOString() 
      : '2024-01-01T00:00:00Z'; // Default: start of 2024
    
    console.log(`📅 Fetching orders since: ${sinceDate}`);

    // Fetch only NEW orders from Shopify
    const shopifyUrl = `https://${storeName}/admin/api/2024-10/orders.json?status=any&created_at_min=${sinceDate}&limit=250`;
    
    const shopifyResponse = await fetch(shopifyUrl, {
      headers: {
        'X-Shopify-Access-Token': accessToken,
        'Content-Type': 'application/json'
      }
    });

    if (!shopifyResponse.ok) {
      throw new Error(`Shopify API error: ${shopifyResponse.status}`);
    }

    const shopifyData = await shopifyResponse.json();
    const orders = shopifyData.orders || [];
    
    console.log(`📦 Fetched ${orders.length} new orders from Shopify`);
    orderProcessingStatus.progress.total = orders.length;

    if (orders.length === 0) {
      orderProcessingStatus.isProcessing = false;
      orderProcessingStatus.lastCompleted = new Date().toISOString();
      orderProcessingStatus.lastResult = {
        ordersProcessed: 0,
        itemsProcessed: 0,
        message: 'No new orders to process'
      };
      
      return res.json({
        success: true,
        ordersProcessed: 0,
        itemsProcessed: 0,
        message: 'All orders up to date!'
      });
    }

    // Process orders in batches to avoid memory issues
    const BATCH_SIZE = 50;
    let totalOrdersInserted = 0;
    let totalItemsInserted = 0;

    for (let i = 0; i < orders.length; i += BATCH_SIZE) {
      const batch = orders.slice(i, i + BATCH_SIZE);
      console.log(`📦 Processing batch ${Math.floor(i / BATCH_SIZE) + 1}/${Math.ceil(orders.length / BATCH_SIZE)}`);
      
      for (const order of batch) {
        try {
          // Hash email for privacy
          const emailHash = order.customer?.email 
            ? crypto.createHash('sha256').update(order.customer.email).digest('hex')
            : null;

          // Insert/update order
          await pool.query(`
            INSERT INTO orders (
              order_id, order_number, customer_id, customer_email_hash,
              total_price, subtotal_price, total_tax, order_date, is_returning_customer
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
            ON CONFLICT (order_id) DO UPDATE SET
              total_price = EXCLUDED.total_price,
              updated_at = NOW()
          `, [
            order.id,
            order.order_number,
            order.customer?.id || null,
            emailHash,
            parseFloat(order.total_price || 0),
            parseFloat(order.subtotal_price || 0),
            parseFloat(order.total_tax || 0),
            order.created_at,
            false // Will be updated later based on customer history
          ]);
          totalOrdersInserted++;

          // Insert line items
          if (order.line_items && order.line_items.length > 0) {
            for (let position = 0; position < order.line_items.length; position++) {
              const item = order.line_items[position];
              
              await pool.query(`
                INSERT INTO order_items (
                  order_id, variant_id, product_id, title, variant_title,
                  quantity, price, cart_position
                ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
                ON CONFLICT (order_id, variant_id, cart_position) DO UPDATE SET
                  quantity = EXCLUDED.quantity,
                  price = EXCLUDED.price
              `, [
                order.id,
                item.variant_id?.toString() || null,
                item.product_id?.toString() || null,
                item.title || 'Unknown',
                item.variant_title || null,
                item.quantity || 1,
                parseFloat(item.price || 0),
                position
              ]);
              totalItemsInserted++;
            }
          }

          orderProcessingStatus.progress.processed++;
        } catch (err) {
          console.error(`❌ Error processing order ${order.id}:`, err.message);
        }
      }

      // Small delay between batches
      await new Promise(resolve => setTimeout(resolve, 100));
    }

    console.log(`✅ Order Blitz complete: ${totalOrdersInserted} orders, ${totalItemsInserted} items`);

    // Mark returning customers
    console.log('🔄 Marking returning customers...');
    await pool.query(`
      UPDATE orders o
      SET is_returning_customer = TRUE
      WHERE customer_id IN (
        SELECT customer_id
        FROM orders
        WHERE customer_id IS NOT NULL
        GROUP BY customer_id
        HAVING COUNT(*) > 1
      )
      AND customer_id IS NOT NULL
    `);

    // Calculate product correlations in background (don't wait for it)
    calculateCorrelations().catch(err => console.error('Correlation error:', err));

    orderProcessingStatus.isProcessing = false;
    orderProcessingStatus.lastCompleted = new Date().toISOString();
    orderProcessingStatus.lastResult = {
      ordersProcessed: totalOrdersInserted,
      itemsProcessed: totalItemsInserted
    };

    res.json({
      success: true,
      ordersProcessed: totalOrdersInserted,
      itemsProcessed: totalItemsInserted,
      message: `Order Blitz complete! Processed ${totalOrdersInserted} orders`
    });

  } catch (error) {
    console.error('❌ Order Blitz error:', error);
    orderProcessingStatus.isProcessing = false;
    res.status(500).json({ error: error.message });
  }
});

// Get order processing status
app.get('/api/orders/status', async (req, res) => {
  res.json(orderProcessingStatus);
});

// Get order analytics from database
app.get('/api/order-analytics', async (req, res) => {
  try {
    const now = new Date();
    const todayStart = new Date(now.getFullYear(), now.getMonth(), now.getDate());
    const weekStart = new Date(now);
    weekStart.setDate(weekStart.getDate() - weekStart.getDay()); // Start of week
    const monthStart = new Date(now.getFullYear(), now.getMonth(), 1);
    const yearStart = new Date(now.getFullYear(), 0, 1);

    // Get aggregated stats
    const statsResult = await pool.query(`
      SELECT 
        COUNT(*) as total_orders,
        SUM(total_price) as total_revenue,
        COUNT(DISTINCT customer_id) as unique_customers,
        SUM(CASE WHEN order_date >= $1 THEN total_price ELSE 0 END) as sales_today,
        COUNT(CASE WHEN order_date >= $1 THEN 1 END) as orders_today,
        SUM(CASE WHEN order_date >= $2 THEN total_price ELSE 0 END) as sales_week,
        COUNT(CASE WHEN order_date >= $2 THEN 1 END) as orders_week,
        SUM(CASE WHEN order_date >= $3 THEN total_price ELSE 0 END) as sales_month,
        COUNT(CASE WHEN order_date >= $3 THEN 1 END) as orders_month,
        SUM(CASE WHEN order_date >= $4 THEN total_price ELSE 0 END) as sales_year,
        COUNT(CASE WHEN order_date >= $4 THEN 1 END) as orders_year
      FROM orders
    `, [todayStart, weekStart, monthStart, yearStart]);

    // Get recent orders with line items
    const ordersResult = await pool.query(`
      SELECT 
        o.order_id,
        o.order_number,
        o.total_price,
        o.order_date,
        json_agg(
          json_build_object(
            'variant_id', oi.variant_id,
            'product_id', oi.product_id,
            'title', oi.title,
            'quantity', oi.quantity,
            'price', oi.price
          )
        ) as line_items
      FROM orders o
      LEFT JOIN order_items oi ON o.order_id = oi.order_id
      GROUP BY o.order_id, o.order_number, o.total_price, o.order_date
      ORDER BY o.order_date DESC
      LIMIT 50
    `);

    const stats = statsResult.rows[0];

    res.json({
      totalOrders: parseInt(stats.total_orders),
      totalRevenue: parseFloat(stats.total_revenue || 0),
      uniqueCustomers: parseInt(stats.unique_customers),
      salesByPeriod: {
        today: parseFloat(stats.sales_today || 0),
        week: parseFloat(stats.sales_week || 0),
        month: parseFloat(stats.sales_month || 0),
        year: parseFloat(stats.sales_year || 0)
      },
      ordersByPeriod: {
        today: parseInt(stats.orders_today),
        week: parseInt(stats.orders_week),
        month: parseInt(stats.orders_month),
        year: parseInt(stats.orders_year)
      },
      recentOrders: ordersResult.rows
    });

  } catch (error) {
    console.error('❌ Order analytics error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Calculate product correlations
async function calculateCorrelations() {
  console.log('🤝 Calculating product correlations...');
  
  try {
    // Clear old correlations
    await pool.query('DELETE FROM product_correlations');

    // Find products bought together
    const result = await pool.query(`
      SELECT 
        oi1.variant_id as variant_a_id,
        oi2.variant_id as variant_b_id,
        COUNT(*) as co_purchase_count
      FROM order_items oi1
      JOIN order_items oi2 ON oi1.order_id = oi2.order_id
      WHERE oi1.variant_id < oi2.variant_id
        AND oi1.variant_id IS NOT NULL
        AND oi2.variant_id IS NOT NULL
      GROUP BY oi1.variant_id, oi2.variant_id
      HAVING COUNT(*) >= 2
    `);

    console.log(`📊 Found ${result.rows.length} product correlations`);

    // Insert correlations
    for (const row of result.rows) {
      await pool.query(`
        INSERT INTO product_correlations (variant_a_id, variant_b_id, co_purchase_count)
        VALUES ($1, $2, $3)
        ON CONFLICT (variant_a_id, variant_b_id) 
        DO UPDATE SET 
          co_purchase_count = EXCLUDED.co_purchase_count,
          last_updated = NOW()
      `, [row.variant_a_id, row.variant_b_id, row.co_purchase_count]);
    }

    console.log('✅ Correlations calculated');
  } catch (error) {
    console.error('❌ Correlation calculation error:', error);
  }
}

// Get product correlations
app.get('/api/correlations', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT 
        pa.title as product_a_name,
        pa.variant_title as product_a_variant,
        pa.barcode as product_a_upc,
        pa.price as product_a_price,
        pb.title as product_b_name,
        pb.variant_title as product_b_variant,
        pb.barcode as product_b_upc,
        pb.price as product_b_price,
        pc.co_purchase_count,
        pc.correlation_score
      FROM product_correlations pc
      JOIN products pa ON pa.variant_id = pc.variant_a_id
      JOIN products pb ON pb.variant_id = pc.variant_b_id
      ORDER BY pc.co_purchase_count DESC
      LIMIT 100
    `);

    const correlations = result.rows.map(r => ({
      productA: {
        name: r.product_a_variant ? `${r.product_a_name} - ${r.product_a_variant}` : r.product_a_name,
        upc: r.product_a_upc,
        price: parseFloat(r.product_a_price || 0)
      },
      productB: {
        name: r.product_b_variant ? `${r.product_b_name} - ${r.product_b_variant}` : r.product_b_name,
        upc: r.product_b_upc,
        price: parseFloat(r.product_b_price || 0)
      },
      timesBoughtTogether: parseInt(r.co_purchase_count),
      correlationScore: parseFloat(r.correlation_score || 0)
    }));

    res.json({ correlations });
  } catch (error) {
    console.error('❌ Correlations error:', error);
    res.status(500).json({ error: error.message });
  }
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'OK', timestamp: new Date().toISOString() });
});

// Start server
async function startServer() {
  console.log('🚀 Starting Store Planner Pro Backend...');
  await initDatabase();
  app.listen(PORT, () => {
    console.log(`✅ Server running on port ${PORT}`);
    console.log(`🔥 Order Blitz: OPTIMIZED & READY`);
    console.log(`📊 Memory-efficient incremental sync enabled`);
  });
}

startServer();
