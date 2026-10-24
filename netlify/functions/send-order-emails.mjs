import { createClient } from '@supabase/supabase-js';

// Netlify scheduled functions run without visitor interaction. Do not expose as a public mail-sending API.
export const config = { schedule: '*/5 * * * *' };

function safeText(s) { return String(s ?? '').replace(/[&<>"']/g, c => ({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;',"'":'&#39;'}[c])); }
function formatMoney(paise) { return new Intl.NumberFormat('en-IN', {style:'currency',currency:'INR'}).format(paise / 100); }
function emailContent(record) {
 const reference = safeText(record.order_number), customer = safeText(record.customer_name), total = formatMoney(record.total_paise);
 const messages = {
  order_placed: ['We received your order', 'Your order has been placed. Payment may still be pending until verified.'],
  payment_received: ['Payment received', 'Your payment has been confirmed. Thank you for choosing Thread in Loops!'],
  status_changed: ['Order update', 'Your order status is now: ' + safeText(record.order_status) + '.']
 };
 const [title,description] = messages[record.event_type] ?? messages.status_changed;
 const subject = 'Thread in Loops • ' + title + ' • ' + record.order_number;
 const html = '<div style="font-family:Arial,sans-serif;max-width:600px;margin:auto;color:#322732"><h2>Thread in Loops 🧶</h2><p>Hello ' + customer + ',</p><p>' + safeText(description) + '</p><p><b>Order:</b> ' + reference + '<br><b>Total:</b> ' + total + '</p><p>For questions, reply to this email.</p><p>Thank you,<br>Thread in Loops</p></div>';
 return {subject,html};
}
export default async () => {
 const { RESEND_API_KEY, EMAIL_FROM, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY } = process.env;
 if (![RESEND_API_KEY, EMAIL_FROM, SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY].every(Boolean)) {
   console.log('[order-email] Mail provider not configured; leaving queued notifications untouched.');
   return new Response('Not configured', {status:200});
 }
 const db = createClient(SUPABASE_URL, SUPABASE_SERVICE_ROLE_KEY, {auth:{persistSession:false,autoRefreshToken:false}});
 const {data:batch,error:claimError} = await db.rpc('claim_order_email',{p_limit:10});
 if(claimError) {console.error('[order-email] Queue claim failed:',claimError.code); return new Response('Queue unavailable',{status:500});}
 let delivered=0;
 for(const item of batch ?? []) {
   try {
     const {subject,html}=emailContent(item);
     const res=await fetch('https://api.resend.com/emails',{
       method:'POST',
       headers:{'Authorization':'Bearer '+RESEND_API_KEY,'Content-Type':'application/json','Idempotency-Key':item.event_key},
       body:JSON.stringify({from:EMAIL_FROM,to:[item.customer_email],subject,html})
     });
     if(!res.ok) throw new Error('Provider HTTP '+res.status);
     const payload=await res.json();
     const {error:updateError}=await db.from('order_email_outbox').update({status:'sent',sent_at:new Date().toISOString(),provider_id:payload.id,last_error:null}).eq('id',item.id);
     if(updateError) throw new Error('Database status update failed');
     delivered++;
   } catch(error) {
     const delayMinutes=Math.min(60,2**Math.min(item.attempts,5));
     await db.from('order_email_outbox').update({status:'failed',available_at:new Date(Date.now()+delayMinutes*60_000).toISOString(),last_error:error instanceof Error ? error.message.slice(0,120):'Unknown delivery error'}).eq('id',item.id);
     console.error('[order-email] Delivery failed. Event ID:',item.id,'Type:',error instanceof Error ? error.message : 'unknown');
   }
 }
 console.log('[order-email] Processed',batch?.length??0,'sent',delivered);
 return new Response(JSON.stringify({processed:batch?.length??0,delivered}),{status:200});
};
