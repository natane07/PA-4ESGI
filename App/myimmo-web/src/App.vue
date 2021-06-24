<template>
  <v-app id="inspire">
    <!-- <v-navigation-drawer
      v-model="drawer"
      app
    >
    </v-navigation-drawer> -->

    <v-app-bar app color="#1e1d1d">
      <!-- <v-app-bar-nav-icon @click="drawer = !drawer"></v-app-bar-nav-icon> -->
      <v-toolbar-title style="color:#e4a362;">
        MyImmo
        <v-btn @click="$router.push({ name: 'Home' })" color="#1e1d1d" style="color:#e4a362;">Home</v-btn>
        <v-btn @click="$router.push({ name: 'Data' })" color="#1e1d1d" style="color:#e4a362;">Data</v-btn>
        </v-toolbar-title>
    </v-app-bar>

    <v-main class="grey lighten-3">
      <v-container
      justify="center"
      no-gutters>
        <v-row>
          <v-col offset>
            <v-sheet
              min-height="85vh"
              rounded="lg"
              outlined
              elevation="7"
              style="padding-top:15px;"
            >
              <router-view/>
            </v-sheet>
          </v-col>
        </v-row>
      </v-container>
    </v-main>

  <v-footer padless color="#1e1d1d">
    <v-row
      justify="center"
      no-gutters
    >
      <v-col
        style="color:#1e1d1d"
        class="lighten-2 py-2 text-center white--text "
        cols="12"
      >
        2021 — <strong>PA 4éme année ESGI</strong>
      </v-col>
    </v-row>
  </v-footer>

  </v-app>
</template>

<script>
  export default {
    data: () => (
        { 
          drawer: false 
        }
      ),
  }
</script>


