{{ config(materialized='ephemeral') }}

-- USPS Publication 28, Appendix B (states and possessions), plus AA/AE/AP.
-- https://pe.usps.com/text/pub28/pub28apb.htm
-- The course references ref('states') but does not supply this reference.
select unnest([
    'AA', 'AE', 'AK', 'AL', 'AP', 'AR', 'AS', 'AZ',
    'CA', 'CO', 'CT', 'DC', 'DE', 'FL', 'FM', 'GA', 'GU',
    'HI', 'IA', 'ID', 'IL', 'IN', 'KS', 'KY', 'LA', 'MA',
    'MD', 'ME', 'MH', 'MI', 'MN', 'MO', 'MP', 'MS', 'MT',
    'NC', 'ND', 'NE', 'NH', 'NJ', 'NM', 'NV', 'NY', 'OH',
    'OK', 'OR', 'PA', 'PR', 'PW', 'RI', 'SC', 'SD', 'TN',
    'TX', 'UT', 'VA', 'VI', 'VT', 'WA', 'WI', 'WV', 'WY'
]) as code
