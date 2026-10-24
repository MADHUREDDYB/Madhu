import { Component, inject, OnInit, signal } from '@angular/core';
import { CommonModule } from '@angular/common';
import { FormsModule } from '@angular/forms';
import { RouterLink } from '@angular/router';
import { createClient, SupabaseClient } from '@supabase/supabase-js';
import { ShopService } from '../shop.service';

interface Address { id:string; label:string; recipient:string; phone:string; address:string; city:string; postal_code:string; }
@Component({
 selector:'app-customer-account',standalone:true,imports:[CommonModule,FormsModule,RouterLink],
 template:`
 <main class="account"><a routerLink="/">← Back to shop</a><h1>My orders & addresses</h1>
 <p>Sign in using the email address you used at checkout. We will send you a secure sign-in link.</p>
 <p class="message" role="status">{{ message() }}</p>
 @if (!loggedIn()) {
 <form (ngSubmit)="sendLink()"><label>Email address <input type="email" required [(ngModel)]="email" name="email" autocomplete="email"></label>
 <button type="submit" [disabled]="busy()">Email my sign-in link</button></form>
 } @else {
 <p>Signed in as {{ email }} <button (click)="signOut()">Sign out</button></p>
 <h2>Past orders</h2>
 @if (!orders().length) { <p>No orders found for this verified email address.</p> }
 @for (o of orders();track o.id) {
 <article><strong>{{ o.order_number }}</strong> · {{ o.created_at | date:'mediumDate' }}<p>{{ (o.total_paise/100) | currency:'INR' }} · {{ o.status }} · Payment: {{ o.payment_status }}</p>
 @for (item of o.order_items || [];track item.id) { <p>{{ item.product_name }} × {{ item.quantity }}</p> }
 </article>}
 <h2>Saved addresses</h2>
 @for (a of addresses();track a.id) {
 <article><strong>{{ a.label }}</strong><p>{{ a.recipient }}, {{ a.address }}, {{ a.city }} - {{ a.postal_code }}</p>
 <button (click)="useAddress(a)">Use for checkout</button> <button (click)="deleteAddress(a.id)">Delete</button></article>}
 <h3>Add address</h3>
 <form (ngSubmit)="saveAddress()">
 <label>Label <input name="label" [(ngModel)]="draft.label" required maxlength="40"></label>
 <label>Full name <input name="recipient" [(ngModel)]="draft.recipient" required minlength="2"></label>
 <label>Mobile <input name="phone" [(ngModel)]="draft.phone" required pattern="[6-9][0-9]{9}"></label>
 <label>Address <textarea name="address" [(ngModel)]="draft.address" required minlength="10"></textarea></label>
 <label>City <input name="city" [(ngModel)]="draft.city" required></label>
 <label>PIN code <input name="postal" [(ngModel)]="draft.postal_code" required pattern="[1-9][0-9]{5}"></label>
 <button [disabled]="busy()" type="submit">Save address</button></form>
 }
 </main>`,
 styles:[`.account{max-width:760px;margin:40px auto;padding:24px;font-family:Arial,sans-serif;color:#49374a}.account h1{font-size:32px}.account article{padding:18px;margin:14px 0;border:1px solid #eadce6;border-radius:12px;background:#fff}.account form{display:grid;gap:12px;margin:20px 0}.account label{display:grid;gap:6px}.account input,.account textarea{font:inherit;padding:11px;border:1px solid #aa97a1;border-radius:7px}.account button{margin:4px;padding:10px 16px;background:#85627b;color:white;border:0;border-radius:8px;cursor:pointer}.message{color:#8e4265}`]
})
export class CustomerAccountComponent implements OnInit {
 private shop=inject(ShopService);
 private client?:SupabaseClient;
 readonly loggedIn=signal(false);readonly busy=signal(false);readonly message=signal('');
 readonly orders=signal<any[]>([]);readonly addresses=signal<Address[]>([]);
 email='';
 draft={label:'Home',recipient:'',phone:'',address:'',city:'',postal_code:''};
 async ngOnInit(){
  try {
   await this.shop.initialize();
   if(this.shop.demoMode()) {this.message.set('Customer accounts require the live Supabase shop.');return;}
   const cfg=this.shop.config();
   this.client=createClient(cfg.supabaseUrl,cfg.supabaseAnonKey,{auth:{detectSessionInUrl:true,persistSession:true}});
   const {data:{session}}=await this.client.auth.getSession();
   if(session){this.loggedIn.set(true);this.email=session.user.email??'';await this.load();}
   this.client.auth.onAuthStateChange((_event,s)=>{
    this.loggedIn.set(!!s);
    if(s){this.email=s.user.email??'';setTimeout(()=>void this.load(),0);}
    else {this.orders.set([]);this.addresses.set([]);}
   });
  } catch {this.message.set('Could not connect to customer accounts.');}
 }
 async sendLink(){
  if(!this.client)return;
  this.busy.set(true);try{
   const {error}=await this.client.auth.signInWithOtp({email:this.email.trim(),options:{emailRedirectTo:location.origin+'/account'}});
   if(error)throw error;this.message.set('Check your inbox for the sign-in link.');
  }catch{this.message.set('Unable to send a sign-in link. Please try again.');}finally{this.busy.set(false);}
 }
 async load(){
  if(!this.client)return;
  const [o,a]=await Promise.all([
   this.client.from('orders').select('id,order_number,total_paise,status,payment_status,created_at,order_items(id,product_name,quantity)').order('created_at',{ascending:false}).limit(50),
   this.client.from('customer_addresses').select('*').order('created_at',{ascending:false})
  ]);
  if(o.error||a.error){this.message.set('Could not load your saved information.');return;}
  this.orders.set(o.data??[]);this.addresses.set((a.data??[]) as Address[]);
 }
 async saveAddress(){
  if(!this.client)return;this.busy.set(true);
  try{
   const {error}=await this.client.from('customer_addresses').insert(this.draft);
   if(error)throw error;
   this.message.set('Address saved.');await this.load();
  }catch{this.message.set('Could not save address. Check the details.');}finally{this.busy.set(false);}
 }
 useAddress(a:Address){
  sessionStorage.setItem('crochet.checkout.address',JSON.stringify({name:a.recipient,phone:a.phone,address:a.address,city:a.city,postalCode:a.postal_code,email:this.email}));
  location.assign('/');
 }
 async deleteAddress(id:string){
  if(!this.client)return;
  const {error}=await this.client.from('customer_addresses').delete().eq('id',id);
  if(error)this.message.set('Could not delete address.');else await this.load();
 }
 async signOut(){await this.client?.auth.signOut();this.loggedIn.set(false);}
}
