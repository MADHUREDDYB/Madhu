-- Customer account: verified email ownership is required before any private order read.
-- Legacy phone-only matching is intentionally NOT supported.
begin;
create function public.current_verified_customer_email() returns text
language sql stable security definer set search_path=''
as $$
 select lower(u.email) from auth.users u
 where u.id=auth.uid() and u.email_confirmed_at is not null
$$;
revoke all on function public.current_verified_customer_email() from public;
grant execute on function public.current_verified_customer_email() to authenticated;

create policy "Verified buyer can read own orders" on public.orders
for select to authenticated
using (customer_email <> '' and lower(customer_email)=public.current_verified_customer_email());
create policy "Verified buyer can read purchased items" on public.order_items
for select to authenticated using (
 exists (select 1 from public.orders o where o.id=order_id and o.customer_email<>'' and lower(o.customer_email)=public.current_verified_customer_email())
);

create table public.customer_addresses (
 id uuid primary key default gen_random_uuid(),
 user_id uuid not null references auth.users(id) on delete cascade default auth.uid(),
 label text not null default 'Home' check (length(label) between 1 and 40),
 recipient text not null check (length(recipient) between 2 and 100),
 phone text not null check (phone ~ '^[6-9][0-9]{9}$'),
 address text not null check (length(address) between 10 and 400),
 city text not null check (length(city) between 2 and 100),
 postal_code text not null check (postal_code ~ '^[1-9][0-9]{5}$'),
 created_at timestamptz not null default now()
);
alter table public.customer_addresses enable row level security;
revoke all on public.customer_addresses from public,anon,authenticated;
grant select,insert,update,delete on public.customer_addresses to authenticated;
create policy "Own addresses select" on public.customer_addresses for select to authenticated using (user_id=auth.uid());
create policy "Own addresses insert" on public.customer_addresses for insert to authenticated with check (user_id=auth.uid() and public.current_verified_customer_email() is not null);
create policy "Own addresses update" on public.customer_addresses for update to authenticated using (user_id=auth.uid()) with check (user_id=auth.uid());
create policy "Own addresses delete" on public.customer_addresses for delete to authenticated using (user_id=auth.uid());
commit;