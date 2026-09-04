<p align="center">
  <img src="https://raw.githubusercontent.com/catalinus26/ha-skylinknet/main/custom_components/skylinknet/brand/icon.png" alt="SkylinkNet" width="120">
</p>

# SkylinkNet Integration for Home Assistant

An unofficial Home Assistant integration for SkylinkNet alarm systems.

## Installation

<details>
<summary><b>HACS (Recommended)</b></summary>

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=catalinus26&repository=ha-skylinknet&category=integration)

### Manual HACS installation

Until the integration is available through HACS:

1. Open HACS in Home Assistant.
2. Click the menu (three dots) in the top right corner.
3. Select **Custom repositories**.
4. Add this repository:
   `https://github.com/catalinus26/ha-skylinknet`
5. Select **Integration** as the category.
6. Click **Add**.
7. Search for **SkylinkNet** and click **Download**.
8. Restart Home Assistant.

</details>

<details>
<summary><b>Manual Installation</b></summary>

1. Download the latest release from the [GitHub Releases](https://github.com/catalinus26/ha-skylinknet/releases) page.
2. Extract the archive.
3. Copy the `skylinknet` folder to your Home Assistant `/config/custom_components/` directory.
4. Restart Home Assistant.

</details>

## Setup

Once installed:

1. Go to **Settings » Devices & Services**.
2. Click **"Add Integration."**
3. Search for **"SkylinkNet."**
4. Enter your SkylinkNet email, password, hub ID, and hub key.


## Compatibility

The integration has been tested and confirmed to work with:

* **SkylinkNet SK-200**
* **PNI SM-400** - sold under the PNI brand

These systems may be sold under different brand/model names while using compatible SkylinkNet hardware and services.

## Known Issues

The following SkylinkNet device types are currently discovered as sensors:

- Light controls (`dev_type 1`) - 
- Appliance/plug controls (`dev_type 2`)
- Remote controls (`dev_type 7`)

Support for these device types is planned for future releases.

## Issues

If you encounter a problem that is not listed under **Known Issues**, please [open an issue](https://github.com/catalinus26/ha-skylinknet/issues).

Be sure to include as much relevant information as possible. This helps with troubleshooting and speeds up the resolution process.

## Disclaimer

This is an unofficial integration and is not affiliated with SkylinkNet. Use at your own risk.



## Buy me a beer 🍺

[https://revolut.me/catali4mw](https://revolut.me/catali4mw)
