We want to have a Raspberry Pi sitting in the center of a router connection. 
We want to some how emulate a real ISP connection, to which the router is connected (probably doing it on my computer). 
To emulate a ISP connection, we are mainly giving it a v4 and v6 address, see what it tries to do, and the Pi could be able to log it. 

```
Hi,

A couple of things, I think. We want to set it up like it would be “in the wild” when connected to a real ISP.

So, that means, probably

Giving it a v4 address via DHCP, and
giving it a v6 prefix via DHCP-Prefix Delegation

And then… seeing what it tries to do.

As we’ve talked about, I think one reason these addresses might repeat is because the NTP server they’re trying to reach out to doesn’t exist (anymore), so seeing what DNS requests and NTP requests they make is interesting.

We also want to see what address it chooses on IPv6, to see if the host portion matches something we see in our data.

Anyway, if you need an NTP server to point it at you can definitely point it to pool.ntp.org. But I think that the interesting bits will be what the router tries to do by default, rather than giving it good time. 
```