# Customer accounts: secure order history and saved addresses
The store still accepts guest checkout. Entering an email or phone number alone MUST NEVER reveal prior orders or saved addresses.

## Activation
1. Apply `supabase/migrations/202610240005_customer_accounts.sql` in Supabase after the base schema.
2. In Supabase → Authentication → Providers, enable Email. Configure email OTP/magic-link delivery and a reliable sender before inviting users. Add the final app origin and `https://deploy-preview-1--threadinloops.netlify.app/account` (for preview testing) under Authentication → URL Configuration → redirect allow list. Review Supabase email-rate limits.
3. Deploy `feature/customer-accounts` to a staging branch site. `/account` is the customer area.
4. Sign in with an email that appears on an existing order. Verify the link, review only that email's orders, add an address and choose Use for checkout. Verify another signed-in customer cannot read those orders or addresses.
5. Keep legacy orders with no email inaccessible; do not match by phone alone. The portal relies on the customer's verified email in Supabase Auth. If the email has changed, a support process is required.
6. Checkout address selection is passed through the user's current tab browser session and must be reviewed by the customer before ordering. No payment/stock or existing admin logic is changed.

## Pending before production
- Run Angular build and automated browser/database tests; none have been executed by this GitHub-only update.
- Add customer-driven edits to saved addresses if needed (currently add/use/delete).
- Review guest-to-account email ownership, email change, verification expiry and rate limits.
- Prevent account sharing between admins and buyers from causing unexpected auth session behavior; Auth clients currently share browser storage.
- Add purchase history paging beyond the 50 most recent orders if necessary.
