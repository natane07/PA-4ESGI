<template>
<div>
  <v-row>
    <v-col
      class="text-center"
      cols="12"
    >
      <h1>Restitution des données immobilieres </h1>
    </v-col>
    <v-col
      class="text-center"
      cols="12"
    >
      <h3><i>Données par regions</i></h3>
    </v-col>
    <v-col
      class="d-flex child-flex"
      cols="4"
    >
      <v-img
        :src="`https://pa-immo-s3.s3.eu-west-3.amazonaws.com/data-vis/moyenne_des_valeurs_fonciere_par_region.png`"
        :lazy-src="`https://pa-immo-s3.s3.eu-west-3.amazonaws.com/data-vis/moyenne_des_valeurs_fonciere_par_region.png`"
        class="grey lighten-2"
      >
      </v-img>
    </v-col>
    <v-col cols="4">
      <v-img
        :src="`https://pa-immo-s3.s3.eu-west-3.amazonaws.com/data-vis/donnee_reparties_france_par_region.PNG`"
        :lazy-src="`https://pa-immo-s3.s3.eu-west-3.amazonaws.com/data-vis/donnee_reparties_france_par_region.PNG`"
        class="grey lighten-2"
      >
      </v-img>
    </v-col>
    <v-col
      class="d-flex child-flex"
      cols="4"
    >
      <v-img
        :src="`https://pa-immo-s3.s3.eu-west-3.amazonaws.com/data-vis/moyenne_du_prix_m2_par_region.png`"
        :lazy-src="`https://pa-immo-s3.s3.eu-west-3.amazonaws.com/data-vis/moyenne_du_prix_m2_par_region.png`"
        aspect-ratio="1"
        class="grey lighten-2"
      >
      </v-img>
    </v-col>
    </v-row>
  <br><br><br>
  <v-row>
    <v-col
      class="text-center"
      cols="12"
    >
      <h3><i>Données par départements</i></h3>
    </v-col>
    <v-col cols="12">
      <v-select
          :items="items"
          v-model="itemsValue"
          solo
          prepend-icon="mdi-map"
        ></v-select>
    </v-col>
    <v-col
      class="d-flex child-flex"
      cols="4"
    >
      <v-img
        :src="`https://pa-immo-s3.s3.eu-west-3.amazonaws.com/data-vis/moyenne_des_valeurs_fonciere_${itemsValue}.png`"
        :lazy-src="`https://pa-immo-s3.s3.eu-west-3.amazonaws.com/data-vis/moyenne_des_valeurs_fonciere_${itemsValue}.png`"
        class="grey lighten-2"

      >
      </v-img>
    </v-col>
    <v-col
      class="d-flex child-flex"
      cols="4"
    >
      <v-img
        :src="`https://pa-immo-s3.s3.eu-west-3.amazonaws.com/data-vis/moyenne_des_surface_bien_${itemsValue}.png`"
        :lazy-src="`https://pa-immo-s3.s3.eu-west-3.amazonaws.com/data-vis/moyenne_des_surface_bien_${itemsValue}.png`"
        class="grey lighten-2"
      >
      </v-img>
    </v-col>
    <v-col
      class="d-flex child-flex"
      cols="4"
    >
      <v-img
        :src="`https://pa-immo-s3.s3.eu-west-3.amazonaws.com/data-vis/moyenne_du_prix_m2_${itemsValue}.png`"
        :lazy-src="`https://pa-immo-s3.s3.eu-west-3.amazonaws.com/data-vis/moyenne_du_prix_m2_${itemsValue}.png`"
        aspect-ratio="1"
        class="grey lighten-2"
      >
      </v-img>
    </v-col>
        <v-col
      cols="12"
      align="center"
    >
      <v-img
        :src="`https://pa-immo-s3.s3.eu-west-3.amazonaws.com/data-vis/donnee_reparties_france_par_departement.PNG`"
        :lazy-src="`https://pa-immo-s3.s3.eu-west-3.amazonaws.com/data-vis/donnee_reparties_france_par_departement.PNG`"
        class="grey lighten-2"
        width="800px"
      >
      </v-img>
    </v-col>
    </v-row>
</div>
</template>

<script>
  export default {
    name: 'HelloWorld',
    data: () => ({
      items: [
        "Ile-de-France", "Centre-Val-de-Loire", "Bourgogne-Franche-Comte", "Normandie", "Hauts-de-France",
        "Grand-Est", "Pays-de-la-Loire", "Bretagne", "Nouvelle-Aquitaine", "Occitanie",
        "Auvergne-Rhone-Alpes", "Provence-Alpes-Cote-d-Azur", "Corse"
      ],
      itemsValue: "Ile-de-France"
    }),
  }
</script>
