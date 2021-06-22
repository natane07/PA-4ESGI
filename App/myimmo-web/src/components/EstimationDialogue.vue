<template>
  <div class="text-center">
    <v-dialog
      v-model="dialog"
      width="500"
    >
      <template v-slot:activator="{ on, attrs }">
        <v-btn
            color="primary"
            dark
            large
            v-bind="attrs"
            v-on="on"
            v-on:click="estimate()"
            
          >
            Estimer mon bien - €
          </v-btn>
      </template>

      <v-card>
        <v-img
            height="250"
            src="https://cdn.vuetifyjs.com/images/cards/house.jpg"
        ></v-img>
        <v-card-title>
            <div class="display-1 mb-2">
                Mon bien immobilier
            </div>
            <div class="title font-weight-regular grey--text" v-if="infoAdresse != undefined">
             <b>Adresse: </b> {{ infoAdresse.properties.label }}
            </div>
            <div class="title font-weight-regular grey--text" v-if="infoAdresse != undefined">
             <b>Coordonées: </b> long: {{ infoAdresse.geometry.coordinates[0] }}, lat: {{infoAdresse.geometry.coordinates[1]}}
            </div>
            <div class="title font-weight-regular grey--text" v-if="surface != undefined">
             <b>Surface du bien: </b> {{ surface }} m²
            </div>
        </v-card-title>

        <v-divider class="mt-6 mx-4"></v-divider>

        <v-card-title v-if="loading == false">Estimation en cours</v-card-title>
        <v-card-title v-else>Estimation</v-card-title>
        <v-card-text>
            <v-row
            justify="center"
            align="center">
              <v-btn
                elevation="17"
                fab
                loading
                v-if="loading == false"
              ></v-btn>
              <div v-else>
                <v-row>
                  <v-col cols="4">
                  <b>Code region :</b> {{ api.code_region }}
                  </v-col>
                  <v-col cols="4">
                    <b>Prix moyen du cartier :</b> {{ api.prix_moyen_cartier }}
                  </v-col>
                  <v-col cols="4">
                    <b>Distance moyenne des biens à proximités :</b> {{ api.distance_moyenne }} km
                  </v-col>
                </v-row>
              <v-row
              justify="center"
              align="center">
              <v-chip 
                class="text-center ma-2"
                color="green"
                outlined
                large
                loading
                >
                    <v-icon left>mdi-currency-eur</v-icon>
                    Valeur estimer : {{ valueEstimate }}€ /m² soit {{valueEstimate * surface }}€ le bien
                </v-chip>
                </v-row>
              </div>

            </v-row>
        </v-card-text>

        <v-divider></v-divider>

        <v-card-actions>
          <v-spacer></v-spacer>
          <v-btn
            color="primary"
            text
            @click="dialog = false"
          >
            Fermer
          </v-btn>
        </v-card-actions>
      </v-card>
    </v-dialog>
  </div>
</template>

<script>
export default {
    data () {
      return {
        dialog: false,
        valueEstimate: 0,
        loading: false,
        api: {}
      }
    },
    props: {
        infoAdresse: Object,
        surface: String
    },
    methods: {
        estimate () {
          console.log(this.infoAdresse)
          console.log(this.surface)
          this.dialog = false
          this.loading = false
          axios.post(
            'http://13.37.61.224:5000/predict',
            {
              latitude: this.infoAdresse.geometry.coordinates[1],
              longitude: this.infoAdresse.geometry.coordinates[0],
              code_departement: (this.infoAdresse.properties.postcode).substr(0,2),
              surface_reelle_bati: this.surface
            },
            {
              headers: {
                'Content-Type': 'application/json'
              }
            }).then((result) => {
              this.dialog = true
              this.loading = true
              console.log(result.data)
              var data = result.data
              this.api = data
              this.valueEstimate = data.prediction
            }).catch(function () {
              console.log('FAILURE!!')
              this.dialog = true
              this.loading = false
            })
        }
    }
}
</script>

<style>

</style>